package com.fth.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession, Dataset}
import org.apache.spark.sql.functions._

object ErrorsCount {
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "FriendsByAge")

    val sesh = SparkSession
      .builder()
      .appName("FriendsByAge")
      .getOrCreate()

    import sesh.implicits._

    val filePath: String = "/Users/shripaddeshpande/Downloads/Errors";
    val filePath2: String = "/Users/shripaddeshpande/Downloads/Errorsout";

    val fileDF: Dataset[String] = sesh.read.textFile(filePath)

    val filtered: DataFrame = fileDF.filter(col("value").isNotNull).filter(s => s.nonEmpty).filter(s => s.length >= 30)
      .map(str => str.substring(0, 30)).groupBy("value").count().sort(desc("count"))

    filtered.write.csv(filePath2)
    filtered.show()
    filtered.printSchema()
  }



}
