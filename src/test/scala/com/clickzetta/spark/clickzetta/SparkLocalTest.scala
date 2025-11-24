package com.clickzetta.spark.clickzetta

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkLocalTest {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("Starting Spark Local Test")
    val spark = SparkSession.builder().appName("SparkLocalTest").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    try {
      // 创建一个简单的测试数据集
      val testData = Seq(
        (1, "test1"),
        (2, "test2"),
        (3, "test3")
      )

      import spark.implicits._
      val df = testData.toDF("id", "value")

      // 执行一些简单的操作
      logger.info("DataFrame Schema:")
      df.printSchema()

      logger.info("DataFrame Content:")
      df.show()

      // 测试一些转换操作
      val filteredDf = df.filter($"id" > 1)
      logger.info("Filtered DataFrame Content:")
      filteredDf.show()


      import org.apache.doris.spark._
      val dorisSparkRDD = sc.dorisRDD(
         tableIdentifier = Some("tpa.table1"),
         cfg = Some(Map(
           "doris.fenodes" -> "node3:8030",
         "doris.request.auth.user" -> "test",
         "doris.request.auth.password" -> "test"
       )))
      dorisSparkRDD.collect()

      logger.info("Test completed successfully!")

    } catch {
      case e: Exception =>
        logger.error("Error occurred during test", e)
        throw e
    } finally {
      logger.info("Stopping Spark session")
      spark.stop()
    }
  }
} 