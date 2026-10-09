package com.clickzetta.connector.source

object SourceReaderFactory {
  def create(sourceFormat: String): SourceReader = {
    Option(sourceFormat).map(_.trim.toLowerCase).getOrElse("") match {
      case "hive" => new HiveSourceReader()
      case "delta" => new DeltaSourceReader()
      case "doris" | "org.apache.doris.spark.sql.sources.dorisdatasource" => new DorisSourceReader()
      case "starrocks" | "com.starrocks.connector.spark.sql.starrocksdatasourceprovider" =>
        new StarRocksSourceReader()
      case unsupported => throw new IllegalArgumentException(s"Unsupported source type: $unsupported")
    }
  }
}
