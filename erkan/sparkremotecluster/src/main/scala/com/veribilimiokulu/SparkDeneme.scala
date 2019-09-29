package com.veribilimiokulu

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.{functions => F}
import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.{functions => F}
import org.apache.spark.SparkConf

object SparkDeneme extends App {

  Logger.getLogger("org").setLevel(Level.INFO) // ilk etapta INFI yaparak alınacak hataların sebepleri öğrenilir.
  // Bütün hatalar ayıklandıktan sonra ERROR'a getirilir

  val sparkConf = new SparkConf()
    .setMaster("yarn")
    .setAppName("SparkDeneme")

    // HDFS konfigürasyonundan "fs.defaultFS" ile aranır. Sunucu adresi HDFS -> Instances -> NameNode'dan öğrenilir.
    .set("spark.hadoop.fs.defaultFS","hdfs://cdh1.impektra.com:8020")

    // YARN konfigürasyonundan "resourcemanager.address" aranır. Sunucu adresi YARN -> Instances -> ResourceManager'dan öğrenilir.
    .set("spark.hadoop.yarn.resourcemanager.address","cdh2.impektra.com:8032")

    // YARN konfigürasyonundan "scheduler.address" aranır. Sunucu adresi YARN -> Instances -> ResourceManager'dan öğrenilir.
    .set("spark.hadoop.yarn.resourcemanager.scheduler.address","cdh2.impektra.com:8030")

    // HDFS -> Instances -> NameNode'dan öğrenilir.
    .set("spark.yarn.jars","hdfs://cdh1.impektra.com:8020/tmp/spark_jars/*.jar")


  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  import spark.implicits._


  val df = spark.read.format("csv")
    .option("header",true)
    .option("inferSchema", true)
    .option("sep",",")
    .load("/user/admin/data/iris.csv")

  df.show()


}
