package local.vbo

/*
Erkan ŞİRİN
ForEachBatch example. Consumes messages from kafka, filters them and produces different kafka topics.
 */
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.Trigger

object readWriteKafkaForEachBatch {
  def main(args: Array[String]): Unit = {

    // Checkpoint directory for kafka write
    val checkpoint_dir = "C:\\tmp\\kafkaCheckPointDir"

   Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("readWriteKafkaForEachBatch")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    import spark.implicits._


    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.206.130:9092")
      .option("subscribe", "input_topic")
      .load()


    // Ham dataframe'in key ve value sütunlarını stringe çevir
    val df2 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .withColumn("input_values", F.split(F.col("value"), ",")(0).cast(StringType))
      .withColumn("output_value", F.split(F.col("value"), ",")(1).cast(StringType))

/*
    val query = df2.writeStream
        .format("console")
        .outputMode("append")
        .start()

    // Produce to kafka
    val query = df2.writeStream
        .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.206.130:9092")
      .option("topic", "output_topic")
      .outputMode("update")
      .option("checkpointLocation", checkpoint_dir)
      .start()

*/
    def myCustomEBFunction( inputDF:DataFrame, batchID:Long ) : Unit = {
      val outlierDF = inputDF.filter(F.col("output_value").equalTo("outlier"))
      outlierDF.show(5)

      val outlierDF2 = outlierDF.withColumn("value",
        F.concat(F.col("input_values"), F.lit(","), F.col("output_value")))

      outlierDF2.select("value").write
        .format("kafka")
        .option("kafka.bootstrap.servers","192.168.206.130:9092")
        .option("topic","outlier")
        .save()


      val inlierDF = inputDF.filter(F.col("output_value").equalTo("normal"))
      inlierDF.show(5)

      val inlierDF2 = inlierDF.withColumn("value",
        F.concat(F.col("input_values"), F.lit(","), F.col("output_value")))

        inlierDF2.select("value").write
        .format("kafka")
        .option("kafka.bootstrap.servers","192.168.206.130:9092")
        .option("topic","normal")
        .save()
}
    val query = df2.writeStream.foreachBatch(myCustomEBFunction _).start()

    query.awaitTermination()
  }
}
