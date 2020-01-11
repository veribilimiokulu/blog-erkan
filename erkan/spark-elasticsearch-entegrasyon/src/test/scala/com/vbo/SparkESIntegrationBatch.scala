package com.vbo

/**
 * Please don't forget add and modify to yourself following dependency in pom.xml
 * <dependency>
 * <groupId>org.elasticsearch</groupId>
 * <artifactId>elasticsearch-spark-20_2.11</artifactId>
 * <version>7.5.0</version>
 * </dependency>
 */

import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.log4j.{Logger, Level}

object SparkESIntegrationBatch extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder
    .master("local[2]")
    .appName("SparkESIntegrationBatch")
    .config("spark.executor.memory", "8g")
    .config("spark.driver.memory", "2g")
    .getOrCreate()

  import spark.implicits._

  println("Spark version", spark.version)

  // Read data
  val df = spark.read.format("csv")
    .option("header", true)
    .option("sep",",")
    .option("inferSchema", true)
    .load("D:\\Datasets\\housing.csv")

  df.show(5)
  df.printSchema()

  // drop nulls
  val df2 = df.na.drop()

  // row counts before drop nulls
  println("row counts before drop nulls: " + df.count())

  // row counts after drop nulls
  println("row counts after drop nulls: " + df2.count())

  // datastructure to keep geolocation (fields must be named lat and lon)
  case class GeoPoint(lat: Double, lon: Double)
  // A function to transform two separate column into one single column (location)
  val makeGoepointForES = (latitude: Double, longitude: Double) => {
    GeoPoint(latitude, longitude)
  }
  // register function
  val makeGoepointForESFunc = F.udf(makeGoepointForES)

  // Put "latitude","longitude" column into one column named location
  val df3 = df2.withColumn("location", makeGoepointForESFunc('latitude, 'longitude))

  // See what happened
  df3.show(5)

  // If transformation operation successfull, drop "latitude","longitude" columns
  val df4 = df3.drop("latitude","longitude")



  // Write to ES
  /*
  df4.write
    .format("org.elasticsearch.spark.sql")
    .mode("overwrite")
    .option("es.nodes", "cloudera")
    .option("es.port","9200")
    .save("housing")

   */

  val dfFromES = spark.read.format("org.elasticsearch.spark.sql")
    .option("es.nodes", "cloudera")
    .option("es.port","9200")
    .load("housing")

  dfFromES.show(5)

}
