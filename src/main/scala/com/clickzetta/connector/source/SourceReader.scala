package com.clickzetta.connector.source

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 数据源读取抽象
 */
trait SourceReader {

  /** 从数据源读取 DataFrame */
  def read(spark: SparkSession, sourceConfigs: Map[String, String],
           sourceTableKey: String, dbTableName: String): DataFrame

  /** 构建 SELECT 列列表（默认 SELECT *，Doris 需要处理 bitmap 列） */
  def buildSelectColumns(sourceDf: DataFrame, sourceConfigs: Map[String, String],
                         sourceTableKey: String, dbTableName: String,
                         enableBitmapToBinary: Boolean): String = "*"
}
