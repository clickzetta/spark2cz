package com.clickzetta.connector.source

object SourceReaderFactory {
  def create(sourceFormat: String): SourceReader = {
    sourceFormat.toLowerCase match {
      case "delta" => new DeltaSourceReader()
      case _ => new DorisSourceReader()
    }
  }
}
