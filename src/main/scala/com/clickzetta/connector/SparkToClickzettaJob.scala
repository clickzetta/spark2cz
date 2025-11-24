package com.clickzetta.connector

import com.clickzetta.spark.clickzetta.ClickzettaOptions
import com.clickzetta.spark.clickzetta.ClickzettaOptions.CZ_TABLE
import org.apache.doris.spark.client.DorisFrontendClient
import org.apache.doris.spark.config.{DorisConfig, DorisOptions}
import org.apache.doris.spark.testcase.TestStreamLoadForArrowType.spark.sqlContext
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.sql.DriverManager
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

// Spark Submit Examples (all configs must have spark. prefix):
// Local mode:
// spark-submit --class com.clickzetta.connector.SparkToClickzettaJob \
//   --conf spark.source.format=org.apache.doris.spark.sql.sources.DorisDataSource \
//   --conf spark.source.table.key=doris.table.identifier \
//   --conf spark.source.table.values=db.table1,db.table2 \
//   --conf spark.source.connector.doris.fenodes=host:8410 \
//   --conf spark.source.connector.doris.user=username \
//   --conf spark.source.connector.doris.password=password \
//   --conf spark.clickzetta.sdk.url=jdbc:clickzetta://host:port/db \
//   --conf spark.clickzetta.sdk.schema=schema_name \
//   spark2cz-1.0-SNAPSHOT.jar
//
// Cluster mode:
// spark-submit --master yarn --deploy-mode cluster \
//   --class com.clickzetta.connector.SparkToClickzettaJob \
//   [... same conf parameters ...] \
//   spark2cz-1.0-SNAPSHOT.jar
object SparkToClickzettaJob {
  private val logger = LoggerFactory.getLogger(this.getClass)

  // Configuration keys (all with spark. prefix to avoid being filtered out by Spark)
  private val SOURCE_FORMAT_KEY = "spark.source.format"
  private val SOURCE_TABLE_KEY_KEY = "spark.source.table.key"
  private val SOURCE_TABLE_VALUES_KEY = "spark.source.table.values"
  private val CZ_SCHEMA_KEY = "spark.clickzetta.sdk.schema"
  private val CZ_ENABLE_RESULTS_VERIFY_KEY = "spark.clickzetta.sdk.enable.results.verify"
  private val CZ_ENABLE_CONCURRENT_COPY_KEY = "spark.clickzetta.sdk.enable.concurrent.copy"
  private val CZ_SAVE_MODE_KEY = "spark.clickzetta.sdk.save.mode"
  private val CZ_ENABLE_BITMAP_CAST_KEY = "spark.clickzetta.sdk.enable.bitmap_to_binary"
  private val SPARK_FILTER_QUERY_KEY = "spark.filter.query"

  // Configuration prefixes (all with spark. prefix to avoid being filtered out by Spark)
  private val SOURCE_CONNECTOR_PREFIX = "spark.source.connector."
  private val CLICKZETTA_SDK_PREFIX = "spark.clickzetta.sdk."

  private case class AppConfig(
                                sourceFormat: String,
                                sourceTableKey: String,
                                sourceTableValues: Array[String],
                                czSchema: String,
                                sourceConfigs: Map[String, String],
                                clickzettaConfigs: Map[String, String],
                                enableResultsVerify: Boolean = false,
                                enableConcurrentCopy: Boolean = false,
                                saveMode: SaveMode = SaveMode.Overwrite,
                                enableBitmapToBinary: Boolean = false,
                                sparkFilterQuery: Option[String] = None
                              )

  private def parseAppConfig(spark: SparkSession): AppConfig = {
    val conf = spark.conf

    // Extract source connector configurations (remove prefix)
    val sourceConfigs = conf.getAll
      .filter(_._1.startsWith(SOURCE_CONNECTOR_PREFIX))
      .map { case (key, value) =>
        val newKey = key.substring(SOURCE_CONNECTOR_PREFIX.length)
        newKey -> value
      }
      .toMap

    // Extract clickzetta SDK configurations (remove prefix), excluding specific control keys
    var clickzettaConfigs = conf.getAll
      .filter(_._1.startsWith(CLICKZETTA_SDK_PREFIX))
      .map { case (key, value) =>
        val newKey = key.substring(CLICKZETTA_SDK_PREFIX.length)
        newKey -> value
      }
      .filterKeys(key =>
        key != "format" &&
        key != "enable.results.verify" &&
        key != "enable.concurrent.copy" &&
        key != "save.mode" &&
        key != "enable.bitmap_to_binary"
      )

    if (!clickzettaConfigs.contains("writeVersionV2SplitUploadEnabled")) {
      clickzettaConfigs += ("writeVersionV2SplitUploadEnabled" -> "true")
    }

    // Parse save mode
    val saveModeStr = conf.getOption(CZ_SAVE_MODE_KEY).getOrElse("overwrite")
    val saveMode = saveModeStr.toLowerCase match {
      case "append" => SaveMode.Append
      case "overwrite" => SaveMode.Overwrite
      case "errorifexists" => SaveMode.ErrorIfExists
      case "ignore" => SaveMode.Ignore
      case _ =>
        logger.warn(s"Unknown save mode: $saveModeStr, using default: overwrite")
        SaveMode.Overwrite
    }

    // Parse spark filter query
    val sparkFilterQuery = conf.getOption(SPARK_FILTER_QUERY_KEY)
      .filter(_.trim.nonEmpty)

    // Get required configurations
    val sourceFormat = conf.get(SOURCE_FORMAT_KEY)
    val sourceTableKey = conf.get(SOURCE_TABLE_KEY_KEY)
    val sourceTableValues = conf.get(SOURCE_TABLE_VALUES_KEY)
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
    val czSchema = conf.get(CZ_SCHEMA_KEY)

    AppConfig(
      sourceFormat = sourceFormat,
      sourceTableKey = sourceTableKey,
      sourceTableValues = sourceTableValues,
      czSchema = czSchema,
      sourceConfigs = sourceConfigs,
      clickzettaConfigs = clickzettaConfigs,
      enableResultsVerify = conf.getOption(CZ_ENABLE_RESULTS_VERIFY_KEY).exists(_.toBoolean),
      enableConcurrentCopy = conf.getOption(CZ_ENABLE_CONCURRENT_COPY_KEY).exists(_.toBoolean),
      saveMode = saveMode,
      enableBitmapToBinary = if (conf.getOption(CZ_ENABLE_BITMAP_CAST_KEY).isEmpty) true else conf.getOption(CZ_ENABLE_BITMAP_CAST_KEY).exists(_.toBoolean),
      sparkFilterQuery = sparkFilterQuery
    )
  }

  private def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("DataToClickzetta")
      .getOrCreate()
  }

  private def createDorisConfig(dorisConfigs: Map[String, String], tableName: String, sourceTableKey: String): DorisConfig = {
    val configMap = (dorisConfigs + (sourceTableKey -> tableName)).asJava
    DorisConfig.fromMap(sqlContext.sparkContext.getConf.getAll.toMap.asJava, configMap, false)
  }

  private def getSourceTableSchema(config: DorisConfig): org.apache.doris.spark.rest.models.Schema = {
    val frontend = new DorisFrontendClient(config)
    val tableIdentifier = config.getValue(DorisOptions.DORIS_TABLE_IDENTIFIER)
    val Array(database, table) = tableIdentifier.split("\\.").map(_.replaceAll("`", ""))
    frontend.getTableSchema(database, table)
  }

  private def createClickzettaTable(
                                     spark: SparkSession,
                                     tableName: String,
                                     clickzettaConfigs: Map[String, String]
                                   ): Unit = {
    val optionsStr = clickzettaConfigs.map { case (key, value) => s"$key '$value'" }.mkString(", ")
    val createTableSql =
      s"""
         |CREATE TABLE IF NOT EXISTS sink_$tableName
         |USING clickzetta
         |OPTIONS (table '$tableName', $optionsStr)
         |""".stripMargin

    logger.info(s"Creating Clickzetta table: $createTableSql")
    spark.sql(createTableSql)
  }

  private def verifyResults(
                             sourceDf: DataFrame,
                             dbTableName: String,
                             tbName: String,
                             appConfig: AppConfig
                           ): Try[Unit] = Try {
    if (!appConfig.enableResultsVerify) {
      logger.info("Results verification is disabled, skipping verification")
      return Success(())
    }

    logger.info(s"Verifying results for table $dbTableName")

    // Query source using Spark SQL
    val futureSource = Future {
      sourceDf.count()
    }

    // Query sink (Clickzetta) using direct JDBC to avoid Spark Literal conversion issues
    val futureSink = Future {
      // Get JDBC URL from clickzetta configs
      val czOptions = new ClickzettaOptions(appConfig.clickzettaConfigs + (CZ_TABLE -> tbName))
      val jdbcUrl = czOptions.url
      val schema = appConfig.czSchema

      // Load JDBC driver
      Class.forName("com.clickzetta.client.jdbc.ClickZettaDriver")

      var connection: java.sql.Connection = null
      var statement: java.sql.Statement = null
      try {
        connection = DriverManager.getConnection(jdbcUrl)
        statement = connection.createStatement()

        // Build SQL with filter if provided
        val whereClause = appConfig.sparkFilterQuery.map(filter => s"WHERE $filter").getOrElse("")
        val countSql = s"SELECT COUNT(*) FROM $schema.`$tbName` $whereClause"

        logger.info(s"Executing direct JDBC query for verification: $countSql")
        val resultSet = statement.executeQuery(countSql)

        if (resultSet.next()) {
          resultSet.getLong(1)
        } else {
          throw new IllegalStateException(s"Could not read count from $schema.`$tbName`")
        }
      } finally {
        if (statement != null) statement.close()
        if (connection != null) connection.close()
      }
    }

    val sourceCount = futureSource.value match {
      case Some(scala.util.Success(v)) => v
      case Some(scala.util.Failure(e)) => throw e
      case None =>
        Await.result(futureSource, Duration.Inf)
    }

    val sinkCount = futureSink.value match {
      case Some(scala.util.Success(v)) => v
      case Some(scala.util.Failure(e)) => throw e
      case None =>
        Await.result(futureSink, Duration.Inf)
    }

    if (sourceCount == sinkCount) {
      logger.info(s"Verification passed: source=$sourceCount, sink=$sinkCount")
    } else {
      throw new RuntimeException(s"Verification failed: source=$sourceCount, sink=$sinkCount")
    }
  }

  private def processTable(
                            spark: SparkSession,
                            dbTableName: String,
                            appConfig: AppConfig,
                            order: Int
                          ): Unit = {
    logger.info(s"Processing the $order-th table: $dbTableName")

    val jobGroup = s"table-$order-${dbTableName.replace(".", "_")}"
    val tbName = dbTableName.split("\\.").last
    
    spark.sparkContext.setJobGroup(
      jobGroup,
      s"Processing table: $dbTableName (${order}/${appConfig.sourceTableValues.length})",
      interruptOnCancel = false
    )
    
    spark.sparkContext.setLocalProperty("spark.job.description", s"Loading data from $dbTableName")
    spark.sparkContext.setLocalProperty("callSite.short", s"Table: $tbName")
    spark.sparkContext.setLocalProperty("callSite.long", s"Processing table $dbTableName (${order}/${appConfig.sourceTableValues.length})")

    logger.info(s"Transformed Doris configurations: ${appConfig.sourceConfigs.keys.mkString(", ")}")

    spark.sparkContext.setLocalProperty("spark.job.description", s"Reading source data from $dbTableName")
    var sourceDf = spark.read
      .format(appConfig.sourceFormat)
      .options(appConfig.sourceConfigs)
      .option(appConfig.sourceTableKey, dbTableName)
      .load()

    if (appConfig.sparkFilterQuery.nonEmpty) {
      sourceDf = sourceDf.filter(appConfig.sparkFilterQuery.get)
    }

    if (sourceDf.isEmpty) {
      logger.warn(s"Table $dbTableName is empty, skipping...")
      spark.sparkContext.clearJobGroup()
      spark.sparkContext.setLocalProperty("spark.job.description", null)
      spark.sparkContext.setLocalProperty("callSite.short", null)
      spark.sparkContext.setLocalProperty("callSite.long", null)
      return
    }

    logger.info(s"Schema for table $dbTableName:")
    sourceDf.schema.printTreeString()

    sourceDf.createOrReplaceTempView(s"source_$tbName")

    spark.sparkContext.setLocalProperty("spark.job.description", s"createOrReplaceTempView for $tbName")
    createClickzettaTable(spark, tbName, appConfig.clickzettaConfigs)

    val dorisConfig = createDorisConfig(appConfig.sourceConfigs, dbTableName, appConfig.sourceTableKey)
    val sourceTableSchema = getSourceTableSchema(dorisConfig)

    // Generate SQL based on SaveMode
    val insertCommand = appConfig.saveMode match {
      case SaveMode.Append => "INSERT INTO"
      case SaveMode.Overwrite => "INSERT OVERWRITE"
      case SaveMode.ErrorIfExists =>
        spark.sparkContext.setLocalProperty("spark.job.description", s"Checking if table sink_$tbName exists (ErrorIfExists mode)")
        // Check if table exists and has data, throw error if it does
        val existingCount = spark.sql(s"SELECT COUNT(*) FROM sink_$tbName").collect()(0).getLong(0)
        if (existingCount > 0) {
          throw new RuntimeException(s"Table sink_$tbName already exists and contains data. SaveMode is ErrorIfExists.")
        }
        "INSERT INTO"
      case SaveMode.Ignore =>
        spark.sparkContext.setLocalProperty("spark.job.description", s"Checking if table sink_$tbName exists (Ignore mode)")
        // Check if table exists and has data, skip if it does
        val existingCount = spark.sql(s"SELECT COUNT(*) FROM sink_$tbName").collect()(0).getLong(0)
        if (existingCount > 0) {
          logger.info(s"Table sink_$tbName already exists and contains data. SaveMode is Ignore, skipping...")
          spark.sparkContext.clearJobGroup()
          spark.sparkContext.setLocalProperty("spark.job.description", null)
          spark.sparkContext.setLocalProperty("callSite.short", null)
          spark.sparkContext.setLocalProperty("callSite.long", null)
          return
        }
        "INSERT INTO"
    }

    import spark.implicits._
    spark.udf.register("bitmapToBytes", (bitmap: String) => {
      if (bitmap != null) {
        // Convert string to bytes using UTF-8 encoding
        bitmap.getBytes("UTF-8")
      } else {
        null
      }
    })

    val insertSql =
      s"""
         |$insertCommand sink_$tbName
         |SELECT
         |  ${
        sourceTableSchema.getProperties.asScala.map { field =>
          if (appConfig.enableBitmapToBinary) {
            if (field.getType.equalsIgnoreCase("bitmap")) {
              s"bitmapToBytes(`${field.getName}`) as `${field.getName}`"
            } else {
              s"`${field.getName}`"
            }
          } else {
            s"`${field.getName}`"
          }
        }.mkString(", ")
      }
         |FROM source_$tbName
         |""".stripMargin
    
    val sqlForDisplay = insertSql.replaceAll("\\s+", " ").trim
    val shortSql = if (sqlForDisplay.length > 200) {
      sqlForDisplay.substring(0, 197) + "..."
    } else {
      sqlForDisplay
    }
    
    spark.sparkContext.setLocalProperty("spark.job.description", s"Executing INSERT for $dbTableName")
    spark.sparkContext.setLocalProperty("callSite.short", s"INSERT: $tbName")
    spark.sparkContext.setLocalProperty("callSite.long", s"Table: $dbTableName\nSQL: $shortSql")
    
    logger.info(s"Executing: $insertSql")

    spark.sql(insertSql).collectAsList()
    logger.info(s"Successfully transferred data to ${appConfig.czSchema}.$tbName")

    if (appConfig.enableResultsVerify) {
      spark.sparkContext.setLocalProperty("spark.job.description", s"Verifying results for $dbTableName")
      spark.sparkContext.setLocalProperty("callSite.short", s"Verify: $tbName")
      spark.sparkContext.setLocalProperty("callSite.long", s"Verifying data transfer results for table $dbTableName")
    }
    
    // Verify results if enabled
    verifyResults(sourceDf, dbTableName, tbName, appConfig) match {
      case Success(_) => logger.info(s"Verification completed for table $dbTableName")
      case Failure(ex) =>
        logger.error(s"Verification failed for table $dbTableName", ex)
        spark.sparkContext.clearJobGroup()
        spark.sparkContext.setLocalProperty("spark.job.description", null)
        spark.sparkContext.setLocalProperty("callSite.short", null)
        spark.sparkContext.setLocalProperty("callSite.long", null)
        throw ex
    }
    
    spark.sparkContext.clearJobGroup()
    spark.sparkContext.setLocalProperty("spark.job.description", null)
    spark.sparkContext.setLocalProperty("callSite.short", null)
    spark.sparkContext.setLocalProperty("callSite.long", null)
  }

  private def printStartupInfo(spark: SparkSession, appConfig: AppConfig): Unit = {
    logger.info("=" * 80)
    logger.info("                    Spark to Clickzetta Job Started")
    logger.info("=" * 80)

    logger.info(s"Configuration Source: Spark Configuration (spark-submit)")
    logger.info(s"Spark Master        : ${spark.sparkContext.master}")
    logger.info(s"Application ID      : ${spark.sparkContext.applicationId}")

    logger.info("-" * 80)
    logger.info("                        Source Configuration")
    logger.info("-" * 80)
    logger.info(s"Source Format        : ${appConfig.sourceFormat}")
    logger.info(s"Source Table Key     : ${appConfig.sourceTableKey}")
    logger.info(s"Tables to Process    : ${appConfig.sourceTableValues.length}")
    appConfig.sourceTableValues.zipWithIndex.foreach { case (table, index) =>
      logger.info(s"  [${index + 1}] $table")
    }

    logger.info("-" * 80)
    logger.info("                      Clickzetta Configuration")
    logger.info("-" * 80)
    logger.info(s"Target Schema        : ${appConfig.czSchema}")
    logger.info(s"Save Mode           : ${appConfig.saveMode}")
    logger.info(s"Results Verification : ${if (appConfig.enableResultsVerify) "ENABLED" else "DISABLED"}")
    logger.info(s"Concurrent Processing: ${if (appConfig.enableConcurrentCopy) "ENABLED" else "DISABLED"}")
    logger.info(s"Bitmap Cast to Binary: ${if (appConfig.enableBitmapToBinary) "ENABLED" else "DISABLED"}")
    logger.info(s"Spark Filter Query   : ${appConfig.sparkFilterQuery.getOrElse("NOT SPECIFIED")}")

    logger.info("-" * 80)
    logger.info("                        Source Connector Settings")
    logger.info("-" * 80)
    appConfig.sourceConfigs.toSeq.sortBy(_._1).foreach { case (key, value) =>
      val displayValue = if (key.toLowerCase.contains("password")) "***" else value
      logger.info(f"  $key%-30s : $displayValue")
    }

    logger.info("-" * 80)
    logger.info("                       Clickzetta SDK Settings")
    logger.info("-" * 80)
    appConfig.clickzettaConfigs.toSeq.sortBy(_._1).foreach { case (key, value) =>
      val displayValue = if (key.toLowerCase.contains("password")) "***" else value
      logger.info(f"  $key%-30s : $displayValue")
    }

    logger.info("-" * 80)
    logger.info("                         Spark Configuration")
    logger.info("-" * 80)
    spark.conf.getAll
      .filter(_._1.startsWith("spark."))
      .toSeq.sortBy(_._1).foreach { case (key, value) =>
        logger.info(f"  $key%-30s : $value")
      }

    logger.info("=" * 80)
  }

  def main(args: Array[String]): Unit = {
    val ss = createSparkSession()
    val appConfig = parseAppConfig(ss)

    // Print formatted startup information
    printStartupInfo(ss, appConfig)

    try {
      val tableProcessing = appConfig.sourceTableValues.zipWithIndex
      
      val successTables = scala.collection.mutable.ListBuffer[String]()
      val failedTables = scala.collection.mutable.ListBuffer[(String, String)]()

      logger.info("Starting data transfer process...")
      logger.info(s"Processing mode: ${if (appConfig.enableConcurrentCopy) "CONCURRENT" else "SEQUENTIAL"}")

      if (appConfig.enableConcurrentCopy) {
        val results = tableProcessing.par.map { case (dbTableName, index) =>
          val order = index + 1
          try {
            processTable(ss, dbTableName, appConfig, order)
            logger.info(s"✓ Successfully processed table $dbTableName ($order/${appConfig.sourceTableValues.length})")
            (dbTableName, true, None)
          } catch {
            case ex: Exception =>
              logger.error(s"✗ Failed to process table $dbTableName ($order/${appConfig.sourceTableValues.length})", ex)
              (dbTableName, false, Some(ex.getMessage))
          }
        }.toArray
        
        results.foreach {
          case (tableName, true, _) => successTables.synchronized { successTables += tableName }
          case (tableName, false, Some(errorMsg)) => failedTables.synchronized { failedTables += ((tableName, errorMsg)) }
          case (tableName, false, None) => failedTables.synchronized { failedTables += ((tableName, "Unknown error")) }
        }
      } else {
        tableProcessing.foreach { case (dbTableName, index) =>
          val order = index + 1
          try {
            processTable(ss, dbTableName, appConfig, order)
            logger.info(s"✓ Successfully processed table $dbTableName ($order/${appConfig.sourceTableValues.length})")
            successTables += dbTableName
          } catch {
            case ex: Exception =>
              logger.error(s"✗ Failed to process table $dbTableName ($order/${appConfig.sourceTableValues.length})", ex)
              failedTables += ((dbTableName, ex.getMessage))
          }
        }
      }

      val successCount = successTables.length
      val failedCount = failedTables.length
      val totalCount = successCount + failedCount

      logger.info("=" * 80)
      logger.info("                         Processing Summary")
      logger.info("=" * 80)
      logger.info(s"Total Tables         : $totalCount")
      logger.info(s"Successfully Processed: $successCount")
      logger.info(s"Failed              : $failedCount")
      logger.info(s"Success Rate        : ${if (totalCount > 0) f"${successCount * 100.0 / totalCount}%.1f%%" else "N/A"}")
      
      if (successCount > 0) {
        logger.info("-" * 80)
        logger.info("                        Successfully Processed Tables")
        logger.info("-" * 80)
        successTables.zipWithIndex.foreach { case (tableName, index) =>
          logger.info(s"  [${index + 1}] $tableName")
        }
      }
      
      if (failedCount > 0) {
        logger.info("-" * 80)
        logger.info("                           Failed Tables")
        logger.info("-" * 80)
        failedTables.zipWithIndex.foreach { case ((tableName, errorMsg), index) =>
          logger.error(s"  [${index + 1}] $tableName")
          logger.error(s"      Error: $errorMsg")
        }
      }

      if (successCount == totalCount) {
        logger.info("Status              : ALL COMPLETED SUCCESSFULLY")
      } else {
        logger.error("Status              : COMPLETED WITH ERRORS!")
        logger.info("=" * 80)
        throw new RuntimeException(s"Job failed: $failedCount out of $totalCount tables failed to process")
      }
      logger.info("=" * 80)

    } catch {
      case e: Exception =>
        logger.error("Critical error during data transfer", e)
        throw e
    } finally {
      logger.info("Stopping Spark session...")
      ss.stop()
      logger.info("Application terminated.")
    }
  }
}