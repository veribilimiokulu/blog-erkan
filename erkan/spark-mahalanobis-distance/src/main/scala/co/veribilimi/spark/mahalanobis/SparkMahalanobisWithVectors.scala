package co.veribilimi.spark.mahalanobis
import breeze.linalg.{DenseVector, DenseMatrix, Matrix, inv}
import org.apache.spark.ml.linalg.{DenseMatrix => SparkDenseMatrix, DenseVector => SparkDenseVector}
import breeze.stats.distributions._
import breeze.stats._
import org.apache.spark.sql.functions.{udf,rand => SQLRandom}
import org.apache.spark.sql.{SparkSession, SQLContext, functions => F}
import scala.util.Random
import org.apache.log4j.{Logger, Level}

object SparkMahalanobisWithVectors {
  def main(args: Array[String]): Unit = {
    println("Welcome to Mahalanobis distance with breeze.linalg")
  Logger.getLogger("org").setLevel(Level.ERROR)
/*
    val normalDist1 = new Gaussian(5,3)

    val x = normalDist1.sample(10).asInstanceOf[DenseVector[Double]]
    println("x: ",x)

    val normalDist2 = new Gaussian(2,2)

    val y = normalDist2.sample(10).asInstanceOf[DenseVector[Double]]
    println("y: ", y)

    println("std of x: ", stddev(x))
    println("std of y: ", stddev(y))

    println("mean of x: ", mean(x))
    println("mean of y: ", mean(y))

    println("variance of x: ", variance(x))
    println("variance of y: ", variance(y))






    println("covariance of x and y: " + covarianceOfTwoVecs(x,y))
    println("covariance of y and x: " + covarianceOfTwoVecs(y,x))
    println("covariance of x and x: " + covarianceOfTwoVecs(x,x))
    println("covariance of y and y: " + covarianceOfTwoVecs(y,y))
    //covarianceOfTwoVecs(x,y)



  println("covMatrix of x,y: " + covMatrixOfTwoVecs(x,y))

    println("inv of covMatrix of x,y: ", inv(covMatrixOfTwoVecs(x,y)))



*/
    /////////////////////////  FUNCTIONS   ////////////////////////////////////////////
    // ==================================================================================
    /***********************   COVARIANCE OF TWO VECTOR  *****************************/
    def covarianceOfTwoVecs(x:DenseVector[Double], y:DenseVector[Double]):Double={

      var myVariance = 0.0
      val x_mean = mean(x)
      val y_mean = mean(y)
      for(i <- 0 to (x.length-1)){
        myVariance = myVariance + ((x(i) - x_mean) * (y(i) - y_mean))
      }

      (1.0/ x.length) * myVariance
    }

    /***************************  DIFFERENCE OF TWO VECTOR  *****************************/
    def diffOfTwoVecs(x:DenseVector[Double], y:DenseVector[Double]):DenseVector[Double]={
      val outputVec = DenseVector.zeros[Double](x.length)
      for(i <- 0 to (x.length-1)){
        outputVec(i) = x(i) - y(i)
      }
      outputVec
    }


    /*********************  COVARIANCE MATRIX OF TWO VECTOR  ***************************/
    def covMatrixOfTwoVecs(x:DenseVector[Double], y:DenseVector[Double]):DenseMatrix[Double]={

      val covMatrix = DenseMatrix.zeros[Double](x.length,y.length)

      for(i <- 0 to (x.length -1)){

        for(j <- 0 to (y.length - 1)){
          if(i == j){
            covMatrix(i,j) = variance(x)
          }else{
            covMatrix(i,j) = covarianceOfTwoVecs(x,y)
          }

        }
      }
      covMatrix(0,0) = variance(x)
      covMatrix(0,1) = covarianceOfTwoVecs(x,y)
      covMatrix(1,0) = covarianceOfTwoVecs(y,x)
      covMatrix(1,1) = variance(y)

      covMatrix
    }




    /////////////////////////    END OF FUNCTIONS   ///////////////////////////////////////
    // ==================================================================================
    /********************************* MAHALANOBIS  ********************************/

    val mahalanobis = (x: SparkDenseVector, y: SparkDenseVector) => {
      val x1 = DenseVector(x.toArray)

      val y1 = DenseVector(y.toArray)


      math.sqrt(diffOfTwoVecs(x1,y1).t * inv(covMatrixOfTwoVecs(x1,y1)) * diffOfTwoVecs(x1,y1))
    }

    // register the udf
    val distanceOfmahalanobis = udf(mahalanobis)




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
   df.show()


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

   df2.select("vectorizedGroup1","vectorizedGroup2").show(false)

  val df3 = df2.withColumn("mahalanobisDistance",  distanceOfmahalanobis('vectorizedGroup1, 'vectorizedGroup2))
  df3.select("mahalanobisDistance").show()

  }
}
