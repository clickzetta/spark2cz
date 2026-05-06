package com.clickzetta.connector.source

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

class DeltaSourceReader extends SourceReader {
  private val logger = LoggerFactory.getLogger(classOf[DeltaSourceReader])

  override def read(spark: SparkSession, sourceConfigs: Map[String, String],
                    sourceTableKey: String, dbTableName: String): DataFrame = {
    sourceConfigs.foreach { case (key, value) =>
      if (key.startsWith("fs.") || key.startsWith("spark.hadoop.")) {
        spark.sparkContext.hadoopConfiguration.set(key, value)
      }
    }
    logger.info(s"Reading Delta table from: $dbTableName")
    spark.read.format("org.apache.spark.sql.delta.sources.DeltaDataSource").load(dbTableName)
  }
}
