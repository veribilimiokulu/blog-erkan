package spark.ml.recommendation.als

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.tuning.{TrainValidationSplit, ParamGridBuilder}
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ErkansALS {
  def main(args: Array[String]): Unit = {

  /* val sparkConf = new SparkConf()
     .setMaster("local[*]")
     .setAppName("SparkALS")
     .setExecutorEnv("spark.driver.memory","4g")
     .setExecutorEnv("spark.executor.memory","10g")
     .setExecutorEnv("spark.sql.broadcastTimeout","1200")
*/
  val spark = SparkSession.builder()
    .master("local[4]")
    .appName("SparkALS")
    //.config("spark.executor.extraJavaOptions","-Xss4g")
    //.config("driver-java-options","-Xss4g")
    .getOrCreate()

    val movieRatings = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\Datasets\\ml-latest-small\\ratings.csv")
      .drop("timestamp")
     // .sample(0.1,142)

   movieRatings.show()


 println(movieRatings.count())

// 100.004 adet rating var.

    // Create training and test set
    val Array(training, test) = movieRatings.randomSplit(Array(0.8, 0.2),seed = 142)
training.cache()

    // Create ALS model
    val alsObject = new ALS()
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
      .setColdStartStrategy("drop")
      .setNonnegative(true)


    // Tune model using ParamGridBuilder
    val paramGridObject = new ParamGridBuilder()
      .addGrid(alsObject.rank, Array(14))
      .addGrid(alsObject.maxIter, Array(20))
      .addGrid(alsObject.regParam, Array(.19))
      .build()

    // Define evaluator as RMSE
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    // Build cross validation using TrainValidationSplit
    val tvs = new TrainValidationSplit()
      .setEstimator(alsObject)
      .setEstimatorParamMaps(paramGridObject)
      .setEvaluator(evaluator)

    // Fit ALS model to training set
    val model = tvs.fit(training)

        // Take best model
    val bestModel = model.bestModel

        // Generate predictions and evaluate RMSE
        val predictions = bestModel.transform(test)
        val rmse = evaluator.evaluate(predictions)

        predictions.show()

        // Print evaluation metrics and model parameters
        println("RMSE = ", rmse)


  }
}
