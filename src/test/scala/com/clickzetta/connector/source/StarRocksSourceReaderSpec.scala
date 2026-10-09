package com.clickzetta.connector.source

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, DataTypes, IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class StarRocksSourceReaderSpec extends AnyFunSuite {
  test("builds bitmap conversion expressions from the source schema") {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("starrocks-source-reader-test")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    try {
      val schema = StructType(Seq(
        StructField("id", DataTypes.IntegerType),
        StructField("bm", DataTypes.StringType)
      ))
      val dataFrame = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, "1,2"))), schema)
      val columns = new StarRocksSourceReader().buildSelectColumns(
        dataFrame,
        Map("starrocks.bitmap.columns" -> "bm"),
        "starrocks.table.identifier",
        "db.table",
        enableBitmapToBinary = true)

      assert(columns == "`id`, bitmapToBytes(`bm`) AS `bm`")
    } finally {
      spark.stop()
    }
  }

  test("forces bitmap schema overrides to string") {
    val reader = new StarRocksSourceReader()
    val schema = StructType.fromDDL(reader.withBitmapStringTypes(
      Some("id INT, bm BIGINT"), Set("bm")))

    assert(schema("id").dataType == IntegerType)
    assert(schema("bm").dataType == StringType)
  }

  test("normalizes nested struct values returned by the RDD reader") {
    val reader = new StarRocksSourceReader()
    val structType = StructType(Seq(
      StructField("name", StringType),
      StructField("values", ArrayType(IntegerType))
    ))
    val raw = Array[AnyRef]("x", java.util.Arrays.asList(Integer.valueOf(1), Integer.valueOf(2)))

    val normalized = reader.normalizeValue(raw, structType).asInstanceOf[Row]

    assert(normalized.getString(0) == "x")
    assert(normalized.getSeq[Int](1) == Seq(1, 2))
  }
}
