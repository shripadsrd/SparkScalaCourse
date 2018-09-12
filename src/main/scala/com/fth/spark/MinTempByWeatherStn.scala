package com.fth.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

// Class to find the minimum temperature by weather station
object MinTempByWeatherStn {

  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MinTempByWeatherStn")

    val sesh = SparkSession
      .builder()
      .appName("MinTempByWeatherStn")
      .getOrCreate()
    import sesh.implicits._
    val filePath: String = "/Users/shripaddeshpande/SparkScala/1800.csv";

    val fields: Array[StructField] =
      Array(StructField("StationID", StringType, nullable = false),
        StructField("Number", StringType, nullable = true),
        StructField("Type", StringType, nullable = false),
        StructField("Temperature", IntegerType, nullable = false))

    val fileDF: DataFrame = sesh.read.schema(StructType(fields)).csv(filePath)

    val stationAndTemp: DataFrame = fileDF.filter("Type = 'TMIN'").select("StationID", "Temperature")
    val minTempByWeatherStn: DataFrame = stationAndTemp.groupBy("StationId").min()

    minTempByWeatherStn.show()
    minTempByWeatherStn.printSchema()
  }

}
