package com.clickzetta.connector

import org.apache.spark.SparkConf

final case class ExportJobConfig(
                                  sourceType: String,
                                  sourceTable: String,
                                  sourceFilter: Option[String],
                                  exportPath: String
                                )

object ExportJobConfig {
  val SourceTypeKey = "spark.spark2cz.source.type"
  val SourceTableKey = "spark.spark2cz.source.table"
  val SourceFilterKey = "spark.spark2cz.source.filter"
  val ExportPathKey = "spark.spark2cz.export.path"

  def fromSparkConf(conf: SparkConf): ExportJobConfig = parse(conf.getAll.toMap)

  private[connector] def parse(values: Map[String, String]): ExportJobConfig = {
    ExportJobConfig(
      sourceType = required(values, SourceTypeKey).toLowerCase,
      sourceTable = required(values, SourceTableKey),
      sourceFilter = values.get(SourceFilterKey).map(_.trim).filter(_.nonEmpty),
      exportPath = required(values, ExportPathKey)
    )
  }

  private def required(values: Map[String, String], key: String): String = {
    values.get(key).map(_.trim).filter(_.nonEmpty).getOrElse {
      throw new IllegalArgumentException(s"Missing required configuration: $key")
    }
  }
}
