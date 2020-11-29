package com.veribilimiokulu

import org.apache.spark.sql.{SparkSession}
import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.{functions => F}
import org.apache.spark.SparkConf

object DilisimBlogSparkKaynakTahsisi extends App {

  val startTime = System.currentTimeMillis()
  println("startTime", startTime)
  Logger.getLogger("org").setLevel(Level.ERROR) // ilk etapta INFO yaparak alınacak hataların sebepleri öğrenilir.
  // Bütün hatalar ayıklandıktan sonra ERROR'a getirilir

  val sparkConf = new SparkConf()
    .setAppName("DilisimBlogSparkKaynakTahsisi")

    //.setMaster("yarn")

    //.set("spark.executor.memory","1500M")
    //.set("spark.executor.cores","2")
    //.set("spark.executor.instances","8")

    // HDFS konfigürasyonundan "fs.defaultFS" ile aranır. Sunucu adresi HDFS -> Instances -> NameNode'dan öğrenilir.
    //.set("spark.hadoop.fs.defaultFS","hdfs://cdh1.impektra.com:8020")

    // YARN konfigürasyonundan "resourcemanager.address" aranır. Sunucu adresi YARN -> Instances -> ResourceManager'dan öğrenilir.
    //.set("spark.hadoop.yarn.resourcemanager.address","cdh2.impektra.com:8032")

    // YARN konfigürasyonundan "scheduler.address" aranır. Sunucu adresi YARN -> Instances -> ResourceManager'dan öğrenilir.
    //.set("spark.hadoop.yarn.resourcemanager.scheduler.address","cdh2.impektra.com:8030")

    // HDFS -> Instances -> NameNode'dan öğrenilir.
    //.set("spark.yarn.jars","hdfs://cdh1.impektra.com:8020/tmp/spark_jars/*.jar")




  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  import spark.implicits._

println(spark.version)
  val df = spark.read.format("csv")
    .option("header",true)
    .option("inferSchema", true)
    .option("sep",",")
    .load("/user/user/vehicle_collisions_nypd.csv")//.repartition(4)

  println("partitionSize: " + df.rdd.partitions.size)
  println("partitionSize: " + df.rdd.getNumPartitions)
  df.show()

  println("df.count: " + df.count())

  df.groupBy(F.col("BOROUGH")).agg(F.countDistinct("ZIP CODE")).show()

  df.groupBy(F.col("CROSS STREET NAME")).agg(F.countDistinct("ON STREET NAME")).show()

  val boroughInjured = df.groupBy(F.col("BOROUGH")).agg(F.sum("PERSONS INJURED"))
  val boroughKilled = df.groupBy(F.col("BOROUGH")).agg(F.sum("PERSONS KILLED"))

  val joined = boroughInjured.join(boroughKilled, boroughInjured.col("BOROUGH") === boroughKilled.col("BOROUGH"))
  joined.show()


  val endTime = System.currentTimeMillis()
  println("endTime", endTime)
  println("Total time: " + (endTime - startTime))

}
