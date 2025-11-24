package com.clickzetta.spark.clickzetta

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.doris.spark.client.DorisFrontendClient
import org.apache.doris.spark.config.{DorisConfig, DorisOptions}
import org.apache.doris.spark.testcase.TestStreamLoadForArrowType.spark.sqlContext
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.io.{File, FileNotFoundException}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

// java -Dclickzetta_config=config/config.properties -Xmx8g -Xms8g -XX:MaxDirectMemorySize=2g -XX:+UseG1GC -jar target/spark2cz-1.0-SNAPSHOT-jar-with-dependencies.jar
// java -Dclickzetta_config=config/config.properties -Xmx8g -Xms8g -XX:MaxDirectMemorySize=2g -XX:+UseG1GC -jar spark2cz-1.0-SNAPSHOT-jar-with-dependencies.jar
// nohup ./openjdk-8/bin/java -Dclickzetta_config=/data1/bi_doris/config.properties -Xmx100g -Xms100g -XX:+UseG1GC -Dlog4j.configuration="/data1/bi_doris/log4j.properties" -XX:MaxDirectMemorySize=10g   -jar /app/spark2cz-1.0-SNAPSHOT-jar-with-dependencies-061117.jar >061117.log 2>&1 &
object SparkToClickzettaJobTest {
  private val logger = LoggerFactory.getLogger(this.getClass)

  // Configuration keys
  private val CONFIG_PATH_ENV = "clickzetta_config"
  private val SOURCE_FORMAT_KEY = "source.format"
  private val SOURCE_TABLE_KEY_KEY = "source.table.key"
  private val SOURCE_TABLE_VALUES_KEY = "source.table.values"
  private val CZ_SCHEMA_KEY = "clickzetta.sdk.schema"
  private val CZ_ENABLE_RESULTS_VERIFY_KEY = "clickzetta.sdk.enable.results.verify"
  private val CZ_ENABLE_CONCURRENT_COPY_KEY = "clickzetta.sdk.enable.concurrent.copy"
  private val CZ_SAVE_MODE_KEY = "clickzetta.sdk.save.mode"
  private val CZ_ENABLE_BITMAP_CAST_KEY = "clickzetta.sdk.enable.bitmap_to_binary"

  // Configuration prefixes
  private val SOURCE_CONNECTOR_PREFIX = "source.connector."
  private val CLICKZETTA_CONNECTOR_PREFIX = "clickzetta.connector."
  private val CLICKZETTA_SDK_PREFIX = "clickzetta.sdk."

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
                                enableBitmapToBinary: Boolean = false
                              )

  private def loadConfig(): Config = {
    val configPath = Option(System.getenv(CONFIG_PATH_ENV))
      .orElse(Option(System.getProperty(CONFIG_PATH_ENV)))

    configPath match {
      case Some(path) =>
        val configFile = new File(path)
        if (configFile.exists()) {
          logger.info(s"Loading configuration from: ${configFile.getAbsolutePath}")
          ConfigFactory.parseFile(configFile).resolve()
        } else {
          throw new FileNotFoundException(s"Configuration file $path not found!")
        }
      case None =>
        logger.info("No external config specified, loading default configuration")
        ConfigFactory.load("config.properties")
    }
  }

  private def parseAppConfig(config: Config): AppConfig = {
    val sourceConfigs = extractConfigs(config, SOURCE_CONNECTOR_PREFIX)
    val clickzettaConfigs = extractConfigs(config, CLICKZETTA_SDK_PREFIX)
      .filterKeys(key => key != "format" && key != "enable.results.verify" && key != "enable.concurrent.copy" && key != "save.mode" && key != "enable.bitmap.cast")

    // Parse save mode
    val saveModeStr = if (config.hasPath(CZ_SAVE_MODE_KEY)) config.getString(CZ_SAVE_MODE_KEY) else "overwrite"
    val saveMode = saveModeStr.toLowerCase match {
      case "append" => SaveMode.Append
      case "overwrite" => SaveMode.Overwrite
      case "errorifexists" => SaveMode.ErrorIfExists
      case "ignore" => SaveMode.Ignore
      case _ =>
        logger.warn(s"Unknown save mode: $saveModeStr, using default: overwrite")
        SaveMode.Overwrite
    }

    AppConfig(
      sourceFormat = config.getString(SOURCE_FORMAT_KEY),
      sourceTableKey = config.getString(SOURCE_TABLE_KEY_KEY),
      sourceTableValues = config.getString(SOURCE_TABLE_VALUES_KEY)
        .split(",")
        .map(_.trim)
        .filter(_.nonEmpty),
      czSchema = config.getString(CZ_SCHEMA_KEY),
      sourceConfigs = sourceConfigs,
      clickzettaConfigs = clickzettaConfigs,
      enableResultsVerify = if (config.hasPath(CZ_ENABLE_RESULTS_VERIFY_KEY)) config.getBoolean(CZ_ENABLE_RESULTS_VERIFY_KEY) else false,
      enableConcurrentCopy = if (config.hasPath(CZ_ENABLE_CONCURRENT_COPY_KEY)) config.getBoolean(CZ_ENABLE_CONCURRENT_COPY_KEY) else false,
      saveMode = saveMode,
      enableBitmapToBinary = if (config.hasPath(CZ_ENABLE_BITMAP_CAST_KEY)) config.getBoolean(CZ_ENABLE_BITMAP_CAST_KEY) else false
    )
  }

  private def extractConfigs(config: Config, prefix: String): Map[String, String] = {
    val configs = config.entrySet().asScala
      .filter(_.getKey.startsWith(prefix))
      .map { entry =>
        val key = entry.getKey.substring(prefix.length)
        val value = entry.getValue.unwrapped().toString
        key -> value
      }
      .toMap

    configs
  }

  private def createSparkSession(config: Config): SparkSession = {
    val builder = SparkSession.builder()
      .appName("DataToClickzetta")
      .master("local[*]")

    // Apply Spark configurations
    extractConfigs(config, CLICKZETTA_CONNECTOR_PREFIX).foreach { case (key, value) =>
      builder.config(key, value)
    }

    builder.getOrCreate()
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
                             spark: SparkSession,
                             dbTableName: String,
                             tbName: String,
                             appConfig: AppConfig
                           ): Try[Unit] = Try {
    if (!appConfig.enableResultsVerify) {
      logger.info("Results verification is disabled, skipping verification")
      return Success(())
    }

    logger.info(s"Verifying results for table $dbTableName")

    val sourceCount = spark.sql(s"SELECT COUNT(*) FROM source_$tbName").collect()(0).getLong(0)
    val sinkCount = spark.sql(s"SELECT COUNT(*) FROM sink_$tbName").collect()(0).getLong(0)

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
                          ): Try[Unit] = Try {
    logger.info(s"Processing the $order-th table: $dbTableName")

    logger.info(s"Transformed Doris configurations: ${appConfig.sourceConfigs.keys.mkString(", ")}")

    val sourceDf = spark.read
      .format(appConfig.sourceFormat)
      .options(appConfig.sourceConfigs)
      .option(appConfig.sourceTableKey, dbTableName)
      .load()

    if (appConfig.enableResultsVerify && sourceDf.isEmpty) {
      logger.warn(s"Table $dbTableName is empty, skipping...")
      return Success(())
    }

    logger.info(s"Schema for table $dbTableName:")
    sourceDf.schema.printTreeString()

    val tbName = dbTableName.split("\\.").last
    sourceDf.createOrReplaceTempView(s"source_$tbName")

    createClickzettaTable(spark, tbName, appConfig.clickzettaConfigs)

    val dorisConfig = createDorisConfig(appConfig.sourceConfigs, dbTableName, appConfig.sourceTableKey)
    val sourceTableSchema = getSourceTableSchema(dorisConfig)

    // Generate SQL based on SaveMode
    val insertCommand = appConfig.saveMode match {
      case SaveMode.Append => "INSERT INTO"
      case SaveMode.Overwrite => "INSERT OVERWRITE"
      case SaveMode.ErrorIfExists =>
        // Check if table exists and has data, throw error if it does
        val existingCount = spark.sql(s"SELECT COUNT(*) FROM sink_$tbName").collect()(0).getLong(0)
        if (existingCount > 0) {
          throw new RuntimeException(s"Table sink_$tbName already exists and contains data. SaveMode is ErrorIfExists.")
        }
        "INSERT INTO"
      case SaveMode.Ignore =>
        // Check if table exists and has data, skip if it does
        val existingCount = spark.sql(s"SELECT COUNT(*) FROM sink_$tbName").collect()(0).getLong(0)
        if (existingCount > 0) {
          logger.info(s"Table sink_$tbName already exists and contains data. SaveMode is Ignore, skipping...")
          return Success(())
        }
        "INSERT INTO"
    }

    spark.sql(s"select * from source_$tbName limit 100").show(false)

    // Test enableBitmapToBinary effect comparison
    logger.info("=" * 60)
    logger.info("Testing enableBitmapToBinary effect comparison:")
    logger.info("=" * 60)

    // Test without bitmap cast (original data)
    logger.info("测试bitmap直接转String的效果：")
    val originalSelectSql =
      s"""
         |SELECT *
         |FROM source_$tbName
         |LIMIT 20
         |""".stripMargin

    logger.info("Query WITHOUT enableBitmapToBinary (original data):")
    logger.info(originalSelectSql)
    spark.sql(originalSelectSql).show(false)

    // Test with bitmap cast (if enabled)
    logger.info("测试 bitmap 字段转换为binary的效果：")
    val bitmapCastSelectSql =
      s"""
         |SELECT
         |  ${
        sourceTableSchema.getProperties.asScala.map { field =>
          if (field.getType.equalsIgnoreCase("bitmap")) {
            s"cast(`${field.getName}` as binary) as `${field.getName}`"
          } else {
            s"`${field.getName}`"
          }
        }.mkString(", ")
      }
         |FROM source_$tbName
         |LIMIT 20
         |""".stripMargin

    logger.info("Query WITH enableBitmapToBinary (bitmap fields cast to binary):")
    logger.info(bitmapCastSelectSql)

    // Execute query and process results for friendly display
    val bitmapCastResult = spark.sql(bitmapCastSelectSql)

    // Convert binary values to string for display

    val displayResult = bitmapCastResult.collect().map { row =>
      val values = row.toSeq.zipWithIndex.map { case (value, index) =>
        val fieldName = bitmapCastResult.schema.fields(index).name
        val fieldType = sourceTableSchema.getProperties.asScala.find(_.getName == fieldName).map(_.getType).getOrElse("")

        if (fieldType.equalsIgnoreCase("bitmap") && value != null) {
          value match {
            case bytes: Array[Byte] => new String(bytes)
            case str: String => str
            case _ => value.toString
          }
        } else {
          value
        }
      }
      values
    }

    logger.info("Bitmap cast result with friendly display (binary converted to string):")
    displayResult.foreach { row =>
      logger.info(s"Row: ${row.mkString(", ")}")
    }


    logger.info("测试 bitmap 直接转String，然后通过udf转换为byte[]的效果：")
    // Test bitmap cast to string then to binary using UDF
    spark.udf.register("bitmapToBytes", (bitmap: String) => {
      if (bitmap != null) {
        // Convert string to bytes using UTF-8 encoding
        bitmap.getBytes("UTF-8")
      } else {
        null
      }
    })

    // Generate SQL to cast bitmap to string then to binary
    val bitmapStringToBytesSelectSql =
      s"""
         |SELECT
         |
         |  ${
        sourceTableSchema.getProperties.asScala.map { field =>
          if (field.getType.equalsIgnoreCase("bitmap")) {
            s"bitmapToBytes(`${field.getName}`) as `${field.getName}`"
          } else {
            s"`${field.getName}`"
          }
        }.mkString(", ")
      }
         |FROM source_$tbName
         |LIMIT 20
         |""".stripMargin

    logger.info("Query WITH bitmap cast to string then to binary:")
    logger.info(bitmapStringToBytesSelectSql)
    val bitmapStringToBytesResult = spark.sql(bitmapStringToBytesSelectSql)
    val displayStringToBytesResult = bitmapStringToBytesResult.collect().map { row =>
      val values = row.toSeq.zipWithIndex.map { case (value, index) =>
        val fieldName = bitmapStringToBytesResult.schema.fields(index).name
        val fieldType = sourceTableSchema.getProperties.asScala.find(_.getName == fieldName).map(_.getType).getOrElse("")

        if (fieldType.equalsIgnoreCase("bitmap") && value != null) {
          value match {
            case bytes: Array[Byte] => new String(bytes, "UTF-8")
            case str: String => str
            case _ => value.toString
          }
        } else {
          value
        }
      }
      values
    }

    logger.info("Bitmap cast to string then to binary result with friendly display:")
    displayStringToBytesResult.foreach { row =>
      logger.info(s"Row: ${row.mkString(", ")}")
    }

    logger.info("=" * 60)

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
          }
        }.mkString(", ")
      }
         |FROM source_$tbName
         |""".stripMargin
    logger.info(s"Executing: $insertSql")

    spark.sql(insertSql).collectAsList()
    logger.info(s"Successfully transferred data to ${appConfig.czSchema}.$tbName")

    // Verify results if enabled
    verifyResults(spark, dbTableName, tbName, appConfig) match {
      case Success(_) => logger.info(s"Verification completed for table $dbTableName")
      case Failure(ex) =>
        logger.error(s"Verification failed for table $dbTableName", ex)
        throw ex
    }
  }

  private def printStartupInfo(config: Config, appConfig: AppConfig): Unit = {
    logger.info("=" * 80)
    logger.info("                    Spark to Clickzetta Job Started")
    logger.info("=" * 80)

    // Configuration file info
    val configPath = Option(System.getenv(CONFIG_PATH_ENV))
      .orElse(Option(System.getProperty(CONFIG_PATH_ENV)))
    logger.info(s"Configuration Source: ${configPath.getOrElse("Default classpath config")}")

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
    val sparkConfigs = extractConfigs(config, CLICKZETTA_CONNECTOR_PREFIX)
    sparkConfigs.toSeq.sortBy(_._1).foreach { case (key, value) =>
      logger.info(f"  $key%-30s : $value")
    }

    logger.info("=" * 80)
  }

  def main(args: Array[String]): Unit = {
    val config = loadConfig()
    val appConfig = parseAppConfig(config)

    // Print formatted startup information
    printStartupInfo(config, appConfig)

    val ss = createSparkSession(config)

    try {
      val tableProcessing = appConfig.sourceTableValues.zipWithIndex

      logger.info("Starting data transfer process...")
      logger.info(s"Processing mode: ${if (appConfig.enableConcurrentCopy) "CONCURRENT" else "SEQUENTIAL"}")

      val results = if (appConfig.enableConcurrentCopy) {
        tableProcessing.par.map { case (dbTableName, index) =>
          val order = index + 1
          processTable(ss, dbTableName, appConfig, order) match {
            case Success(_) =>
              logger.info(s"✓ Successfully processed table $dbTableName ($order/${appConfig.sourceTableValues.length})")
              true
            case Failure(ex) =>
              logger.error(s"✗ Failed to process table $dbTableName ($order/${appConfig.sourceTableValues.length})", ex)
              false
          }
        }.toArray
      } else {
        tableProcessing.map { case (dbTableName, index) =>
          val order = index + 1
          processTable(ss, dbTableName, appConfig, order) match {
            case Success(_) =>
              logger.info(s"✓ Successfully processed table $dbTableName ($order/${appConfig.sourceTableValues.length})")
              true
            case Failure(ex) =>
              logger.error(s"✗ Failed to process table $dbTableName ($order/${appConfig.sourceTableValues.length})", ex)
              false
          }
        }
      }

      val successCount = results.count(identity)
      val totalCount = results.length

      logger.info("=" * 80)
      logger.info("                         Processing Summary")
      logger.info("=" * 80)
      logger.info(s"Total Tables         : $totalCount")
      logger.info(s"Successfully Processed: $successCount")
      logger.info(s"Failed              : ${totalCount - successCount}")
      logger.info(s"Success Rate        : ${if (totalCount > 0) f"${successCount * 100.0 / totalCount}%.1f%%" else "N/A"}")

      if (successCount == totalCount) {
        logger.info("Status              : ALL COMPLETED SUCCESSFULLY")
      } else {
        logger.warn("Status              : COMPLETED WITH ERRORS!")
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