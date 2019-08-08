package co.veribilimi.spark.densevectorToRDD

import org.apache.spark.ml.linalg.{DenseMatrix => SparkDenseMatrix, DenseVector => SparkDenseVector}
import breeze.linalg.{DenseVector}
import org.apache.spark.sql.SparkSession

object SparkDenseVectorToRDD {
  def main(args: Array[String]): Unit = {

   // Create SparkDenseVector
    val sparkDenseVecFromArray = new SparkDenseVector(Array(1,2,3,4,5,6,7,8))
    println(sparkDenseVecFromArray)

    // Create breeze DenseVector from SparkDenseVector
    val breezeDenseVecFromSparkDV = DenseVector(sparkDenseVecFromArray.toArray)
    println(breezeDenseVecFromSparkDV)

    // Create SparkSession
    val spark = SparkSession.builder().master("local[4]").getOrCreate()
    import spark.implicits._

    // Create an RDD from
    val myRDD = spark.createDataset(breezeDenseVecFromSparkDV.toArray).rdd
    myRDD.take(8).foreach(println)
  }
}
