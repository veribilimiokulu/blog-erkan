package mallcustomers
import org.apache.spark.ml.{PipelineModel, Pipeline}
import org.apache.spark.ml.feature.{OneHotEncoderEstimator,VectorAssembler, StandardScaler,PCA,PCAModel,StringIndexer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}

object PCAExampleMallCustomers {
  def main(args: Array[String]): Unit = {
    //Create spark session
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("ComputeKForPCA")
      .getOrCreate()

    // load iris data
    val data = spark.read.format("csv").option("inferSchema", "true").option("header", "true")
      .load("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\Datasets\\Mall_Customers.csv")

    // Check the dataframe
    data.show()


    // Null control
    val colNames = data.columns
    var sayac = 1
    val nullsArray = Array[String]()
    val nonullArray = Array[String]()
    for (col <- colNames){
      if(data.filter(data.col(col).isNull).count() > 0){
        nullsArray+col
      }else{
        nonullArray+col
      }
      sayac+=1
    }
    nullsArray.foreach(println)
    nonullArray.foreach(println)

    // StringIndexer
    def stringIndexerPipeline(inputCol: String): (Pipeline, String) = {
      val indexer = new StringIndexer()
        .setHandleInvalid("skip")
        .setInputCol(inputCol)
        .setOutputCol(inputCol + "_indexed")
      val pipeline = new Pipeline().setStages(Array(indexer))
      (pipeline, inputCol + "_indexed")
    }

    /*


        // PCA - A Function that computes explained variance
        def DetermineLDANumber(df:DataFrame, k_pca:Int): PipelineModel = {

          // Preprocessing categorical features
          val (genderPipeline, gender_indexed) = stringIndexerPipeline("gender")

          // Use StringIndexer output as input for OneHotEncoderEstimator
          val oneHotEncoder = new OneHotEncoderEstimator()
            //.setDropLast(true)
            //.setHandleInvalid("skip")
            .setInputCols(Array("gender_indexed"))
            .setOutputCols(Array("gender_indexedVec"))


          // Gather features that will be pass through pipeline
          val OHECols = oneHotEncoder.getOutputCols ++ Array("age","annual_income","spending_score")

          // Put all inputs in a column as a vector
          val vectorAssembler = new VectorAssembler().
            setInputCols(OHECols).
            setOutputCol("featureVector")

          // Scale vector column
          val standartScaler = new StandardScaler()
            .setInputCol("featureVector")
            .setOutputCol("scaledFeatureVector")
            .setWithStd(true)
            .setWithMean(false)

          // Create a PCA object
          val pca = new PCA()
            .setInputCol("scaledFeatureVector")
            .setOutputCol("PCAScaledFeatureVector")
            .setK(k_pca)

          // Create a pipeline and sort estimators
          val pipeline = new Pipeline().setStages(
            Array(genderPipeline, oneHotEncoder, vectorAssembler, standartScaler, pca))
          pipeline.fit(df)

        }
        //end of function

        // Finding optimal k number
        val exvar = Array[Double]()
        for (k_pca <- 2 to 4 by 1) {

          val pipelineModel = DetermineLDANumber(data, k_pca)

          val transformedDF = pipelineModel.transform(data)
          val pcaModel = pipelineModel.stages.last.asInstanceOf[PCAModel]
          val explainedVariance = pcaModel.explainedVariance
          println("k value:" + k_pca, "Explained Variance: " + explainedVariance, explainedVariance.toArray.sum)
          exvar:+explainedVariance.toArray.sum
        }
        //exvar.foreach(println)
        // We determine the k_pca value as 3

        */

    //Let's continue clustering

    // KMeans Clustering - A Function that computes Kmeans
    def ComputeKMeansModel(df:DataFrame,  k:Int, k_pca:Int): PipelineModel = {

      // Preprocessing categorical features
      val (genderPipeline, gender_indexed) = stringIndexerPipeline("gender")

      // Use StringIndexer output as input for OneHotEncoderEstimator
      val oneHotEncoder = new OneHotEncoderEstimator()
        //.setDropLast(true)
        //.setHandleInvalid("skip")
        .setInputCols(Array("gender_indexed"))
        .setOutputCols(Array("gender_indexedVec"))


      // Gather features that will be pass through pipeline
      val OHECols = oneHotEncoder.getOutputCols ++ Array("age","annual_income","spending_score")

      // Put all inputs in a column as a vector
      val vectorAssembler = new VectorAssembler()
        .setInputCols(OHECols)
        .setOutputCol("featureVector")

      // Scale vector column
      val standartScaler = new StandardScaler()
        .setInputCol("featureVector")
        .setOutputCol("scaledFeatureVector")
        .setWithStd(true)
        .setWithMean(false)

      // Create a PCA object
      val pca = new PCA()
        .setInputCol("scaledFeatureVector")
        .setOutputCol("PCAScaledFeatureVector")
        .setK(k_pca)

      val kmeansObject = new KMeans()
        .setSeed(142)
        .setK(k)
        .setPredictionCol("cluster")
        .setFeaturesCol("PCAScaledFeatureVector")
        .setMaxIter(40)
        .setTol(1.0e-5)

      // Create a pipeline and sort estimators
      val pipeline = new Pipeline().setStages(
        Array(genderPipeline, oneHotEncoder, vectorAssembler, standartScaler, pca, kmeansObject))
      pipeline.fit(df)

    }
    //end of function

    // Determine optimal k value for KMeans
    data.cache()
    import org.apache.spark.ml.evaluation.ClusteringEvaluator
    val evaluator = new ClusteringEvaluator()
      .setFeaturesCol("PCAScaledFeatureVector")
      .setPredictionCol("cluster")
      .setMetricName("silhouette")

    for(k <- 2 to 10 by 1){
      val pipelineModel =  ComputeKMeansModel(data,k,3)

      val transformedDF = pipelineModel.transform(data)

      val score = evaluator.evaluate(transformedDF)
      val kmeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]
      println(k,score,kmeansModel.computeCost(transformedDF))
    }
  }
}
