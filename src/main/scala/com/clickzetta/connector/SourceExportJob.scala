package com.clickzetta.connector

import com.clickzetta.connector.source.SourceReaderFactory
import org.apache.spark.sql.SparkSession

object SourceExportJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    try {
      val config = ExportJobConfig.fromSparkConf(spark.sparkContext.getConf)
      val sourceReader = SourceReaderFactory.create(config.sourceType)
      var source = sourceReader.read(spark, Map.empty, "path", config.sourceTable)
      config.sourceFilter.foreach(filter => source = source.filter(filter))
      source.write.mode("overwrite").parquet(config.exportPath)
    } finally {
      spark.stop()
    }
  }
}
