package com.clickzetta.connector.source

import org.scalatest.funsuite.AnyFunSuite

class SourceReaderFactorySpec extends AnyFunSuite {
  test("create supported source readers") {
    assert(SourceReaderFactory.create("hive").isInstanceOf[HiveSourceReader])
    assert(SourceReaderFactory.create("DELTA").isInstanceOf[DeltaSourceReader])
    assert(SourceReaderFactory.create("doris").isInstanceOf[DorisSourceReader])
    assert(SourceReaderFactory.create("org.apache.doris.spark.sql.sources.DorisDataSource")
      .isInstanceOf[DorisSourceReader])
    assert(SourceReaderFactory.create("starrocks").isInstanceOf[StarRocksSourceReader])
    assert(SourceReaderFactory.create("com.starrocks.connector.spark.sql.StarRocksDataSourceProvider")
      .isInstanceOf[StarRocksSourceReader])
  }

  test("reject unknown source type") {
    val exception = intercept[IllegalArgumentException] {
      SourceReaderFactory.create("unknown")
    }

    assert(exception.getMessage.contains("unknown"))
  }
}
