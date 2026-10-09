package com.clickzetta.connector.source

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 *
 配置                                                 作用
 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 spark.source.connector.doris.read.bitmap-to-string=true  让 Doris Connector 以字符串形式读取 bitmap
 ───────────────────────────────────────────────────  ──────────────────────────────────────────────────────────────────
 spark.clickzetta.sdk.enable.bitmap_to_binary=true    为 bitmap 字段启用 Spark Binary 中间承载和后续类型匹配
 ───────────────────────────────────────────────────  ──────────────────────────────────────────────────────────────────
 cz.bitmap.type.enable=true                           ClickZetta Row/Arrow 写入链路默认启用 ClickZetta bitmap 类型标记
 */
class DorisSourceReader extends SourceReader {
  private val logger = LoggerFactory.getLogger(classOf[DorisSourceReader])

  override def read(spark: SparkSession, sourceConfigs: Map[String, String],
                    sourceTableKey: String, dbTableName: String): DataFrame = {
    spark.read
      .format(sourceConfigs.getOrElse("__format__", "org.apache.doris.spark.sql.sources.DorisDataSource"))
      .options(sourceConfigs.filter { case (k, _) => !k.startsWith("__") })
      .option(sourceTableKey, dbTableName)
      .load()
  }

  override def buildSelectColumns(sourceDf: DataFrame, sourceConfigs: Map[String, String],
                                  sourceTableKey: String, dbTableName: String,
                                  enableBitmapToBinary: Boolean): String = {
    if (!enableBitmapToBinary) return "*"

    val bitmapCols = getBitmapColumns(sourceConfigs, sourceTableKey, dbTableName)
    if (bitmapCols.isEmpty) return "*"

    sourceDf.schema.fieldNames.map { col =>
      if (bitmapCols.contains(col)) s"bitmapToBytes(`$col`) as `$col`" else s"`$col`"
    }.mkString(", ")
  }

  /** 通过反射调用 Doris API 获取 bitmap 列，避免 delta profile 编译问题 */
  private def getBitmapColumns(sourceConfigs: Map[String, String], sourceTableKey: String,
                               dbTableName: String): Set[String] = {
    try {
      val configMap = (sourceConfigs.filter { case (k, _) => !k.startsWith("__") } + (sourceTableKey -> dbTableName)).asJava
      val dorisConfigClass = Class.forName("org.apache.doris.spark.config.DorisConfig")
      val fromMap = dorisConfigClass.getMethod("fromMap", classOf[java.util.Map[_, _]], classOf[java.util.Map[_, _]], java.lang.Boolean.TYPE)
      val dorisConfig = fromMap.invoke(null, java.util.Collections.emptyMap(), configMap, java.lang.Boolean.FALSE)

      val frontendClass = Class.forName("org.apache.doris.spark.client.DorisFrontendClient")
      val frontend = frontendClass.getConstructor(dorisConfigClass).newInstance(dorisConfig)

      val optionsClass = Class.forName("org.apache.doris.spark.config.DorisOptions")
      val identifierKey = optionsClass.getField("DORIS_TABLE_IDENTIFIER").get(null)
      val tableId = dorisConfigClass.getMethod("getValue", identifierKey.getClass).invoke(dorisConfig, identifierKey).toString
      val Array(db, table) = tableId.split("\\.").map(_.replaceAll("`", ""))

      val schema = frontendClass.getMethod("getTableSchema", classOf[String], classOf[String]).invoke(frontend, db, table)
      val props = schema.getClass.getMethod("getProperties").invoke(schema).asInstanceOf[java.util.List[_]]

      props.asScala.flatMap { field =>
        val t = field.getClass.getMethod("getType").invoke(field).toString
        val n = field.getClass.getMethod("getName").invoke(field).toString
        if ("bitmap".equalsIgnoreCase(t)) Some(n) else None
      }.toSet
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to get bitmap columns for $dbTableName: ${e.getMessage}")
        Set.empty[String]
    }
  }
}
