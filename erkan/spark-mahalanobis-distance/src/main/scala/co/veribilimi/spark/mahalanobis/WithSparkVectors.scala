package co.veribilimi.spark.mahalanobis

import org.apache.spark.ml.linalg.{Vectors, DenseMatrix => SparkDenseMatrix, DenseVector => SparkDenseVector, _}
import breeze.stats.distributions._
import breeze.stats._
import org.apache.spark.sql.functions.{udf, rand => SQLRandom}
import org.apache.spark.sql.{SQLContext, SparkSession, functions => F}

import scala.util.Random
import org.apache.log4j.{Level, Logger}

object WithSparkVectors extends App {
  println("Welcome to Mahalanobis distance with spark.ml.linalg")
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().master("local[4]").getOrCreate()

  import spark.implicits._

  // create synthetic data
  val df = spark.range(0, 100)
    .select("id")
    .withColumn("vecCol1", SQLRandom(10L))
    .withColumn("vecCol2", SQLRandom(11L))
    .withColumn("vecCol3", SQLRandom(12L))
    .withColumn("vecCol4", SQLRandom(13L))
    .withColumn("vecCol5", SQLRandom(14L))
    .withColumn("vecCol6", SQLRandom(15L))
    .withColumn("vecCol7", SQLRandom(16L))
    .withColumn("vecCol8", SQLRandom(17L))
    .withColumn("vecCol9", SQLRandom(18L))
    .withColumn("vecCol10", SQLRandom(19L))
    .withColumn("vecCol11", SQLRandom(20L))
    .withColumn("vecCol12", SQLRandom(21L))

  // show synthetic data
  //df.show()


  // Split data into 2 dataframe with vectorized cols
  val vectorGroup1 = Array[String]("vecCol1","vecCol2","vecCol3","vecCol4","vecCol5","vecCol6")
  val vectorGroup2 = Array[String]("vecCol7","vecCol8","vecCol9","vecCol10","vecCol11","vecCol12")

  import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}

  val assembler = new VectorAssembler()
    .setInputCols(vectorGroup1)
    .setOutputCol("vectorizedGroup1")

  val df1 = assembler.transform(df)

  val assembler2 = new VectorAssembler()
    .setInputCols(vectorGroup2)
    .setOutputCol("vectorizedGroup2")

  val df2 = assembler2.transform(df1)

  //df2.select("vectorizedGroup1","vectorizedGroup2").show(false)

  // yukarıdaki udf'i kullanarak noktalar ile küme merkezlerinin mesafesini hesapla
  val distancesDF = df2.withColumn("distance", distFromCenter($"vectorizedGroup1", $"vectorizedGroup2"))

  distancesDF.show()











  /***************************  DIFFERENCE OF TWO VECTOR  *****************************/
  // Cluster merkezlerini temsil eden vektör ile noktaları temsil eden vektör arasındaki mesafeyi hesaplayan fonksiyonu tanımla
  val distFromCenter = F.udf(
                        (v1: Vector, v2:Vector) =>
                          (Vectors.sqdist( v1, v2))
  )



}

