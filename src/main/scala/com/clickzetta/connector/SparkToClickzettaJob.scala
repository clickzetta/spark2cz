package com.clickzetta.connector

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.io.{File, FileNotFoundException}
import scala.collection.JavaConverters._

// java -Xmx8g -Xms8g -XX:MaxDirectMemorySize=2g -XX:+UseG1GC -jar target/spark2cz-1.0-SNAPSHOT-jar-with-dependencies.jar
object SparkToClickzettaJob {
  private val logger = LoggerFactory.getLogger(this.getClass)

  // Source configuration
  var SOURCE_FORMAT: String = _
  var SOURCE_TABLE_KEY: String = _
  var SOURCE_TABLE_VALUES: Array[String] = _

  // Clickzetta connection configuration 
  var CZ_ENDPOINT: String = _
  var CZ_USERNAME: String = _
  var CZ_PASSWORD: String = _
  var CZ_WORKSPACE: String = _
  var CZ_VIRTUAL_CLUSTER: String = _
  var CZ_SCHEMA: String = _
  var CZ_ACCESS_MODE: String = _
  var CZ_FORMAT: String = _

  def loadConfig(): Config = {
    val configPath = System.getProperty("clickzetta_config")
    if (configPath != null) {
      val configFile = new File(configPath)
      if (configFile.exists()){
        logger.info("=====================================")
        logger.info(s"The user config file path is : ${configFile.getAbsolutePath}")
        logger.info("=====================================")
        logger.info(s"Loading configuration from external file: ${configFile.getAbsolutePath}")
        ConfigFactory.parseFile(configFile).resolve()
      } else {
        throw new FileNotFoundException(s"configuration file $configPath not found!")
      }
    } else {
      logger.info("Loading configuration from classpath config.properties")
      ConfigFactory.load("config.properties")
    }
  }

  def init(): Unit = {
    val config = loadConfig()

    // Load source configuration
    SOURCE_FORMAT = config.getString("source.format")
    SOURCE_TABLE_KEY = config.getString("source.table.key")
    SOURCE_TABLE_VALUES = config.getString("source.table.values").split(",").map(_.trim)

    // Load Clickzetta configuration
    CZ_ENDPOINT = config.getString("clickzetta.endpoint")
    CZ_USERNAME = config.getString("clickzetta.username")
    CZ_PASSWORD = config.getString("clickzetta.password")
    CZ_WORKSPACE = config.getString("clickzetta.workspace")
    CZ_VIRTUAL_CLUSTER = config.getString("clickzetta.virtualCluster")
    CZ_SCHEMA = config.getString("clickzetta.schema")
    CZ_ACCESS_MODE = config.getString("clickzetta.access_mode")
    CZ_FORMAT = config.getString("clickzetta.format")

    logger.info("Configuration loaded successfully")
    logger.info(s"Will process these tables: ${SOURCE_TABLE_VALUES.mkString(", ")}")
  }

  def getConfigs(config: Config, prefix: String): Map[String, String] = {
    val configs = config.entrySet().asScala
      .filter(_.getKey.startsWith(prefix))
      .map(entry => {
        val key = entry.getKey.replace(prefix, "")
        (key, entry.getValue.unwrapped().toString)
      })
      .toMap

    logger.info(s"Loaded configurations with prefix '$prefix': ${configs.keys.mkString(", ")}")
    configs
  }

  def main(args: Array[String]): Unit = {
    logger.info("Starting data transfer application")

    val config = loadConfig()
    init()

    val spark = SparkSession.builder()
      .appName("DataToClickzetta")
      .master("local[*]")

    // Apply all spark configurations from config file
    getConfigs(config, "clickzetta.connector.").foreach { case (key, value) =>
      logger.info(s"Setting Spark config: $key = $value")
      spark.config(key, value)
    }

    val sparkSession = spark.getOrCreate()

    val sourceConfigs = getConfigs(config, "source.connector.")
    logger.info(s"Source configurations: $sourceConfigs")

    try {
      var order = 0
      for (dbTableName <- SOURCE_TABLE_VALUES) {
        order += 1
        try {
          logger.info(s"Processing the $order-th table: $dbTableName")

          val sourceDf = sparkSession.read
            .format(SOURCE_FORMAT)
            .options(sourceConfigs)
            .option(SOURCE_TABLE_KEY, dbTableName)
            .load()

          if (sourceDf.isEmpty) {
            logger.warn(s"Table $dbTableName is empty, skipping...")
          } else {
            logger.info(s"Schema for table $dbTableName:")
            sourceDf.schema.printTreeString()

            val rowCount = sourceDf.count()
            logger.info(s"Successfully read $rowCount rows from table $dbTableName")

            if (rowCount > 0) {
              // Write to Clickzetta
              logger.info(s"Starting to write data from $dbTableName to Clickzetta table: $CZ_SCHEMA.$dbTableName")
              val tableName = dbTableName.split("\\.").last
              sourceDf.write
                .format(CZ_FORMAT)
                .option("endpoint", CZ_ENDPOINT)
                .option("username", CZ_USERNAME)
                .option("password", CZ_PASSWORD)
                .option("workspace", CZ_WORKSPACE)
                .option("virtualCluster", CZ_VIRTUAL_CLUSTER)
                .option("schema", CZ_SCHEMA)
                .option("table", tableName)
                .option("access_mode", CZ_ACCESS_MODE)
                .mode("append")
                .save()

              logger.info(s"Successfully completed data transfer to table $CZ_SCHEMA.$tableName")
            } else {
              logger.warn(s"No data found in table $dbTableName, skipping write operation")
            }
          }
        } catch {
          case e: Exception =>
            logger.error(s"Error processing table $dbTableName", e)
            logger.info(s"Continuing with next table, has processed $order tables.")
        }
      }

      logger.info("Successfully completed all data transfers")

    } catch {
      case e: Exception =>
        logger.error("Error occurred during data transfer", e)
        throw e
    } finally {
      logger.info("Stopping Spark session")
      sparkSession.stop()
    }
  }
}