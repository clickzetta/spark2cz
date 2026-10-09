package com.clickzetta.connector.source

import com.starrocks.connector.spark._
import com.starrocks.connector.spark.sql.schema.InferSchema
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StringType, StructField, StructType}

import scala.collection.JavaConverters._

/**
 * Reads StarRocks tables through the StarRocks Spark connector.
 *
 * Non-Bitmap tables use the connector DataFrame path. Bitmap tables use the
 * connector's public RDD path so the FE can project bitmap_to_string().
 */
class StarRocksSourceReader extends SourceReader {
  override def read(spark: SparkSession, sourceConfigs: Map[String, String],
                    sourceTableKey: String, dbTableName: String): DataFrame = {
    val options = connectorOptions(sourceConfigs)
    val bitmapColumns = parseColumns(sourceConfigs.getOrElse("starrocks.bitmap.columns", ""))
    val dataFrame = if (bitmapColumns.nonEmpty) {
      readWithRdd(spark, options, dbTableName, bitmapColumns)
    } else {
      val format = sourceConfigs.get("__format__").filter(_.trim.nonEmpty).getOrElse("starrocks")
      spark.read
        .format(format)
        .options(options)
        .option(sourceTableKey, dbTableName)
        .load()
    }

    options.get("starrocks.cast.largeint.columns")
      .toSeq
      .flatMap(_.split(",").map(_.trim).filter(_.nonEmpty))
      .foldLeft(dataFrame) { (current, columnName) =>
        current.columns.find(_.equalsIgnoreCase(columnName)) match {
          case Some(actualName) => current.withColumn(actualName, col(actualName).cast("long"))
          case None => current
      }
    }
  }

  override def buildSelectColumns(sourceDf: DataFrame, sourceConfigs: Map[String, String],
                                  sourceTableKey: String, dbTableName: String,
                                  enableBitmapToBinary: Boolean): String = {
    if (!enableBitmapToBinary) return "*"
    val bitmapColumns = parseColumns(sourceConfigs.getOrElse("starrocks.bitmap.columns", ""))
    if (bitmapColumns.isEmpty) return "*"
    sourceDf.schema.fieldNames.map { columnName =>
      if (containsColumn(bitmapColumns, columnName)) s"bitmapToBytes(`${escape(columnName)}`) AS `${escape(columnName)}`"
      else s"`${escape(columnName)}`"
    }.mkString(", ")
  }

  private def readWithRdd(spark: SparkSession, options: Map[String, String], dbTableName: String,
                         bitmapColumns: Set[String]): DataFrame = {
    val schemaOptions = options ++ Map(
      "starrocks.table.identifier" -> dbTableName,
      "starrocks.column.types" -> withBitmapStringTypes(options.get("starrocks.column.types"), bitmapColumns))
    val schema = InferSchema.inferSchema(schemaOptions.asJava)
    val readFields = schema.fieldNames.map { columnName =>
      val quoted = s"`${escape(columnName)}`"
      if (containsColumn(bitmapColumns, columnName)) s"bitmap_to_string($quoted) AS $quoted" else quoted
    }.mkString(",")
    val rddOptions = options ++ Map(
      "starrocks.fenodes" -> options.getOrElse("starrocks.fenodes", feNodes(options)),
      "starrocks.request.auth.user" -> options.getOrElse("starrocks.user", ""),
      "starrocks.request.auth.password" -> options.getOrElse("starrocks.password", ""),
      "starrocks.read.field" -> readFields
    )
    val rows = spark.sparkContext
      .starrocksRDD(tableIdentifier = Some(dbTableName), cfg = Some(rddOptions))
      .map(value => Row.fromSeq(asSeq(value, schema)))
    spark.createDataFrame(rows, schema)
  }

  private def asSeq(value: Any, schema: StructType): Seq[Any] = value match {
    case values: java.util.List[_] =>
      require(values.size == schema.length, s"StarRocks RDD row has ${values.size} values, expected ${schema.length}")
      values.asScala.zip(schema.fields).map { case (item, field) => normalizeValue(item, field.dataType) }
    case values: scala.collection.Seq[_] =>
      require(values.size == schema.length, s"StarRocks RDD row has ${values.size} values, expected ${schema.length}")
      values.zip(schema.fields).map { case (item, field) => normalizeValue(item, field.dataType) }
    case other => throw new IllegalArgumentException(s"Unexpected StarRocks RDD row type: ${other.getClass.getName}")
  }

  private[source] def normalizeValue(value: Any, dataType: DataType): Any = {
    if (value == null) return null
    dataType match {
      case ArrayType(elementType, _) => value match {
        case values: java.lang.Iterable[_] => values.asScala.map(item => normalizeValue(item, elementType)).toSeq
        case values: scala.collection.Seq[_] => values.map(item => normalizeValue(item, elementType))
        case values: Array[_] => values.toSeq.map(item => normalizeValue(item, elementType))
        case other => other
      }
      case MapType(keyType, valueType, _) => value match {
        case values: java.util.Map[_, _] => values.asScala.map { case (key, item) =>
          normalizeValue(key, keyType) -> normalizeValue(item, valueType)
        }.toMap
        case values: scala.collection.Map[_, _] => values.map { case (key, item) =>
          normalizeValue(key, keyType) -> normalizeValue(item, valueType)
        }
        case other => other
      }
      case structType: StructType => value match {
        case row: Row =>
          require(row.length == structType.length, s"StarRocks struct has ${row.length} values, expected ${structType.length}")
          Row.fromSeq(structType.fields.zipWithIndex.map { case (field, index) =>
          normalizeValue(row.get(index), field.dataType)
        })
        case values: Array[_] =>
          require(values.length == structType.length, s"StarRocks struct has ${values.length} values, expected ${structType.length}")
          Row.fromSeq(structType.fields.zip(values).map { case (field, item) =>
          normalizeValue(item, field.dataType)
        })
        case values: java.util.List[_] =>
          require(values.size == structType.length, s"StarRocks struct has ${values.size} values, expected ${structType.length}")
          Row.fromSeq(structType.fields.zip(values.asScala).map { case (field, item) =>
          normalizeValue(item, field.dataType)
        })
        case other => other
      }
      case _ => value
    }
  }

  private[source] def withBitmapStringTypes(configured: Option[String], bitmapColumns: Set[String]): String = {
    val configuredFields = configured.map(_.trim).filter(_.nonEmpty)
      .map(StructType.fromDDL(_).fields.toSeq).getOrElse(Seq.empty)
      .filterNot(field => containsColumn(bitmapColumns, field.name))
    val bitmapFields = bitmapColumns.toSeq.sorted.map(column => StructField(column, StringType, nullable = true))
    StructType(configuredFields ++ bitmapFields).toDDL
  }

  private def connectorOptions(sourceConfigs: Map[String, String]): Map[String, String] = {
    sourceConfigs.filter { case (key, _) =>
      !key.startsWith("__") && key != "starrocks.bitmap.columns"
    }
  }

  private def parseColumns(value: String): Set[String] =
    value.split(",").map(_.trim).filter(_.nonEmpty).toSet

  private def containsColumn(columns: Set[String], value: String): Boolean =
    columns.exists(_.equalsIgnoreCase(value))

  private def feNodes(options: Map[String, String]): String =
    options.getOrElse("starrocks.fe.http.url", "").replaceFirst("(?i)^https?://", "").split("/", 2).head

  private def escape(value: String): String = value.replace("`", "``")
}
