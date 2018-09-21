package com.fth.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object PopularMovie {

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCount")

    val sesh = SparkSession
      .builder()
      .appName("PopularMovie")
      .getOrCreate()
    import sesh.implicits._
    val filePath: String = "/Users/shripaddeshpande/workspace/ml-100k/u.data";

    val fields: Array[StructField] =
      Array(StructField("UserID", IntegerType, nullable = false),
        StructField("MovieID", IntegerType, nullable = false),
        StructField("Rating", IntegerType, nullable = false),
        StructField("Timestamp", StringType, nullable = false))

    val file: Dataset[String] = sesh.read.option("delimiter", "\t").textFile(filePath)
    val movies: DataFrame = file.map(x => (x.split("\t")(1).toInt)).withColumn("MovieID", col("value")).drop("value")

    val groupedMovies: Dataset[Row] = movies.groupBy("MovieID").count().sort(desc("count"))

    val movieNames = MovieNames.loadMovieNames(sesh)

    val joinedMovies: DataFrame = groupedMovies.join(movieNames, "MovieID")

    joinedMovies.printSchema()
    joinedMovies.show()

  }

}
