package com.fth.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object WordCount {

  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCount")

    val sesh = SparkSession
      .builder()
      .appName("WordCount")
      .getOrCreate()
    import sesh.implicits._
    val filePath: String = "/Users/shripaddeshpande/SparkScala/book.txt";
    //NOTE: read.text returns DATAFRAME which does not work with flatmap
    val fileDf: Dataset[String] = sesh.read.textFile(filePath)
    val wordCount: DataFrame = fileDf.flatMap(row => row.split(" ")).groupBy("Value").count()

    wordCount.printSchema()
    wordCount.show()
  }

}
