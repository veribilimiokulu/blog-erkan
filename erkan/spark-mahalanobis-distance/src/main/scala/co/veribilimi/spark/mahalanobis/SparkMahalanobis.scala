package co.veribilimi.spark.mahalanobis

import breeze.linalg._
import scala.util.Random
import scala.math._

object SparkMahalanobis {
  def main(args: Array[String]): Unit = {
    println("Wellcome to Spark Scala Mahalanobis Distance")

/*********************  OBJECT   *************************************/
    object Computations{

      def computeStd(x:Seq[Int]):Double={
        // input: Seq of integers
        // output: Double as std
        var x_mean:Double = (x.sum.asInstanceOf[Double] / x.length.asInstanceOf[Double])
        var sumOfSquares = 0.0
        x.foreach( i => {
          sumOfSquares = sumOfSquares + pow(2,(i - x_mean))
        })
        sqrt(sumOfSquares/(x.length))

      }

      def meanOfSeq(x:Seq[Int]):Double={

        (x.sum.asInstanceOf[Double] / x.length.asInstanceOf[Double])
      }

      def computeVariance(x:Seq[Int]):Double={
        // input: Seq of integers
        // output: Double as std
        var x_mean:Double = meanOfSeq(x)
        var sumOfSquares = 0.0
        x.foreach( i => {
          sumOfSquares = sumOfSquares + pow(2,(i - x_mean))
        })
        (sumOfSquares/(x.length))

      }

      def computeCovariance(x:Seq[Int], y:Seq[Int]):Double={
        var outputCovariance = 0.0
        var sumOfTotalDiffFromMean = 0.0
        var x_mean = meanOfSeq(x)
        var y_mean = meanOfSeq(y)
        for(i <- 0 to x.length-1){
          sumOfTotalDiffFromMean = sumOfTotalDiffFromMean + ((x(i).asInstanceOf[Double] - x_mean)*(y(i).asInstanceOf[Double] - y_mean))
        }

        outputCovariance = (1/x.length)*sumOfTotalDiffFromMean
        outputCovariance
      }


    }

    /***************************  OBJECT END  *************************************/


    // Generate 10 random integer between 0-10
    Random.setSeed(142)
    val x = Seq.fill(10)(Random.nextInt(10))
    println("Array of x:")
    x.foreach(println)
    val y = Seq.fill(10)(Random.nextInt(10))
    println("Array of y:")
    y.foreach(println)

    // computation standard deviation for an Seq
    val x_mean = Computations.meanOfSeq(x)
    val y_mean = Computations.meanOfSeq(y)
    println("x_mean: ", x_mean)
    println("y_mean: ", y_mean)


    val x_std = Computations.computeStd(x)
    val y_std = Computations.computeStd(y)
    println("x_std: ", x_std)
    println("y_std: ", y_std)

    val x_var = Computations.computeVariance(x)
    val y_var = Computations.computeVariance(y)
    println("x_var: ", x_var)
    println("y_var: ", y_var)

    val xy_cov = Computations.computeCovariance(x,y)
    println("xy_cov: ",xy_cov)


  }
}
