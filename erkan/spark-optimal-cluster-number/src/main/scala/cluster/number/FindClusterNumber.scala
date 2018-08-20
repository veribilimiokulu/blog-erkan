package cluster.number
import org.apache.spark.ml.{PipelineModel, Pipeline}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.{DataFrame, SparkSession}

object FindClusterNumber {
  def main(args: Array[String]): Unit = {
    //Create spark session
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("FindClusterNumber")
      .getOrCreate()

    // load iris data
    val data = spark.read.format("csv").option("inferSchema","true").option("header","true")
      .load("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\Datasets\\iris.csv")

    // Check the dataframe
    data.show()

    // The function below returns pipelineModel
    def ComputeKMeansModel(df:DataFrame, k:Int): PipelineModel = {

     // Pick up input columns
      val inputColumns = df.columns.filter(_!="Species")

      val vectorAssembler = new VectorAssembler().
        setInputCols(inputColumns).
        setOutputCol("featureVector")

      val standartScaler = new StandardScaler()
        .setInputCol("featureVector")
        .setOutputCol("scaledFeatureVector")
        .setWithStd(true)
        .setWithMean(false)

      val kmeansObject = new KMeans().
        setSeed(142).
        setK(k).
        setPredictionCol("cluster").
        setFeaturesCol("scaledFeatureVector").
        setMaxIter(40).
        setTol(1.0e-5)

      val pipeline = new Pipeline().setStages(
        Array(vectorAssembler, standartScaler, kmeansObject))
      pipeline.fit(df)

    }
    //end of function

    // Finding optimal k number

    val evaluator = new ClusteringEvaluator()
      .setFeaturesCol("scaledFeatureVector")
      .setPredictionCol("cluster")
      .setMetricName("silhouette")

    for(k <- 2 to 7 by 1){
      val pipelineModel =  ComputeKMeansModel(data,k)

      val transformedDF = pipelineModel.transform(data)

      val score = evaluator.evaluate(transformedDF)
      val kmeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]
      println("k value:"+k, "Score: "+score, "KMeans ComputeCost: "+ kmeansModel.computeCost(transformedDF))
    }


  }
}
