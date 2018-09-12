package com.fth.spark

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Compute the average number of friends by age in a social network. */
object FriendsByAge {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "FriendsByAge")

    val sesh = SparkSession
      .builder()
      .appName("FriendsByAge")
      .getOrCreate()

    val filePath: String = "/Users/shripaddeshpande/SparkScala/fakefriends.csv";

    val fields: Array[StructField] =
      Array(StructField("ID", IntegerType, nullable = false),
      StructField("Name", StringType, nullable = false),
      StructField("Age", IntegerType, nullable = false),
      StructField("Friends", IntegerType, nullable = false))

    val fileDF: DataFrame = sesh.read.schema(StructType(fields)).csv(filePath)

    val averageFriendsByAge: DataFrame = fileDF.groupBy("Age").avg("Friends")
    averageFriendsByAge.show()

  }

}
