package com.fth.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType

object PopularSuperHero {

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCount")

    val sesh = SparkSession
      .builder()
      .appName("PopularSuperHero")
      .getOrCreate()
    import sesh.implicits._
    val filePathForGraph: String = "/Users/shripaddeshpande/SparkScala/Marvel-graph.txt";
    val filePathForNames: String = "/Users/shripaddeshpande/SparkScala/Marvel-names.txt";

    val heroNames: Dataset[String] = sesh.read.textFile(filePathForNames)
    val superHeroNames: DataFrame = heroNames.withColumn("HeroID", split(col("value"), " ").getItem(0))
      .withColumn("HeroName", split(col("value"), " ").getItem(1))
      .drop("value")

    val heroGraph: Dataset[String] = sesh.read.textFile(filePathForGraph)

    // Adding Friends column as an array
    val heroFriends =
      heroGraph.withColumn("HeroID", split(col("value"), " ").getItem(0))
      .withColumn("Friends", split(col("value"), " "))
        .drop("value")

//    heroFriends.printSchema()
//    heroFriends.show()

    // we need to group all the hero ids together
    val friends: DataFrame = heroFriends.groupBy("HeroID").count().sort(desc("count"))
//    friends.printSchema()
//    friends.show()

    val groupedFriends: DataFrame = heroFriends.groupBy("HeroID").agg(col("Friends"))

    groupedFriends.printSchema()
    groupedFriends.show()

  }
}
