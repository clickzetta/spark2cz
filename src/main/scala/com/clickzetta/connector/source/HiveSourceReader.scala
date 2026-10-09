package com.clickzetta.connector.source

import org.apache.spark.sql.{DataFrame, SparkSession}

class HiveSourceReader extends SourceReader {
  override def read(spark: SparkSession, sourceConfigs: Map[String, String],
                    sourceTableKey: String, dbTableName: String): DataFrame = {
    spark.table(dbTableName)
  }
}
