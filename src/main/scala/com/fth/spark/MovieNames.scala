package com.fth.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object MovieNames {

  def loadMovieNames (sesh: SparkSession): DataFrame = {
    import sesh.implicits._
    val filePath: String = "/Users/shripaddeshpande/workspace/ml-100k/u.item";

    val movieNames: Map[Int, String] = Map()
    val file: Dataset[String] = sesh.read.textFile(filePath)

    val splits: DataFrame = file.withColumn("MovieID", split(col("value"), "\\|").getItem(0))
      .withColumn("MovieName", split(col("value"), "\\|").getItem(1))
      .drop("value")

    return splits
  }

}
