package com.clickzetta.connector

import org.scalatest.funsuite.AnyFunSuite

class ExportJobConfigSpec extends AnyFunSuite {
  test("parse required export configuration and optional filter") {
    val config = ExportJobConfig.parse(Map(
      ExportJobConfig.SourceTypeKey -> " HIVE ",
      ExportJobConfig.SourceTableKey -> " db.orders ",
      ExportJobConfig.SourceFilterKey -> " dt = '2026-08-03' ",
      ExportJobConfig.ExportPathKey -> " cosn://bucket-appid/cmt/task "
    ))

    assert(config.sourceType == "hive")
    assert(config.sourceTable == "db.orders")
    assert(config.sourceFilter.contains("dt = '2026-08-03'"))
    assert(config.exportPath == "cosn://bucket-appid/cmt/task")
  }

  test("treat blank filter as absent") {
    val config = ExportJobConfig.parse(requiredValues + (ExportJobConfig.SourceFilterKey -> "  "))

    assert(config.sourceFilter.isEmpty)
  }

  test("reject missing required configuration") {
    val exception = intercept[IllegalArgumentException] {
      ExportJobConfig.parse(requiredValues - ExportJobConfig.SourceTableKey)
    }

    assert(exception.getMessage.contains(ExportJobConfig.SourceTableKey))
  }

  private val requiredValues = Map(
    ExportJobConfig.SourceTypeKey -> "hive",
    ExportJobConfig.SourceTableKey -> "db.orders",
    ExportJobConfig.ExportPathKey -> "cosn://bucket-appid/cmt/task"
  )
}
