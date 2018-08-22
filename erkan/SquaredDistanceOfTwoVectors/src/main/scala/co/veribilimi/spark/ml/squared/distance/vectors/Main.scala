package co.veribilimi.spark.ml.squared.distance.vectors
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.{Column, DataFrame, SparkSession, UDFRegistration}
import org.apache.spark.sql.functions._



object Main {
  def main(args: Array[String]): Unit = {

    // create a spark session
  val spark = SparkSession.builder()
    .master("local")
    .appName("vector_distance")
    .getOrCreate()


    // load data
    val data5 = spark.read.format("csv").option("inferSchema","true").option("header","true")
      .load("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\Datasets\\iris.csv")

    // Check the dataframe
    //data5.show()

    // The function below returns pipelineModel
    def ComputeClusteringModel(df:DataFrame, k:Int): PipelineModel = {

      // Pick up input columns (we do it just by excluding target feature)
      val inputColumns = df.columns.filter(_!="Species")

      // create a vectorAssembler put all those input. Creates a new column vector named featureVector
      val vectorAssembler = new VectorAssembler().
        setInputCols(inputColumns).
        setOutputCol("featureVector")

      // create a scaler and scale all inputs. Gets in what vectorAssembler outs and creates new column as vector but scaled one
      // named scaledFeatureVector
      val standartScaler = new StandardScaler()
        .setInputCol("featureVector")
        .setOutputCol("scaledFeatureVector")
        .setWithStd(true)
        .setWithMean(false)

      // create clustering object. Gets in what standartScaler outs.
      val kmeansObject = new KMeans().
        setSeed(142).
        setK(k).
        setPredictionCol("cluster").
        setFeaturesCol("scaledFeatureVector").
        setMaxIter(40).
        setTol(1.0e-5)

      // Create a pipeline object and sort all estimators created above in logical order, dependencies
      val pipeline = new Pipeline().setStages(
        Array(vectorAssembler, standartScaler, kmeansObject))

      // Train pipeline ad return pipelineModel full of trained estimators created above
      pipeline.fit(df)

    }
    //end of function

    // Finding optimal k number
    val evaluator = new ClusteringEvaluator()
      .setFeaturesCol("scaledFeatureVector")
      .setPredictionCol("cluster")
      .setMetricName("silhouette")

    // We compute 2 - 7 and check out which number is greater(closeness to 1)
    for(k <- 2 to 7 by 1){

      // Get a pipelineModel by using our function with loop k
      val pipelineModel =  ComputeClusteringModel(data5,k)

      // transform our pipeline model and get new dataframe with new features
      val transformedDF = pipelineModel.transform(data5)

      // get evaluater score as double. the dataframe must have scaledFeatureVector column which we declared while
      // creating evaluator object
      val score = evaluator.evaluate(transformedDF)
      val kmeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]
      println("k value:"+k, "Score: "+score, "KMeans ComputeCost: "+ kmeansModel.computeCost(transformedDF))
    }

    // We choose 3 as optimal cluster number
    val pipelineModel =  ComputeClusteringModel(data5,3)

    // Pick up clustering model from pipeline stages. It is one of them. Last one.
    val kmeanModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]

    // transform pipelinemodel and get new computed features like cluster number.
    // We can see which iris is located in which cluster
    val transformedDF = pipelineModel.transform(data5)

    // COMPUTE THE DISTENCE OF EACH IRIS FLOWER TOWARDS ITS CLUSTER CENTER EUCLIDIAN DISTANCE

    // get cluster center from clustering model which is a vector (think it as coordinates)
    val clusterCenters = kmeanModel.clusterCenters

    import spark.implicits._

    // we match cluster centers with it's cluster number.
    val vec = clusterCenters.zipWithIndex

    // array to seq
    val vecDF = vec.toSeq

    // seq to df
    val seqDF = vecDF.toDS

    //check df if it is ok
    seqDF.show()

    // join newly created cluster center and cluster number dataframe with the transformed one from original.
    // be aware you must use left join. transformed (big one) must be on the left
    val data6 = transformedDF.join(seqDF, transformedDF("cluster") === seqDF("_2"), "left")
      .withColumnRenamed("_1","clusterCenter")
        .drop("_2")


    // Create a usr defined function which computes distance two vectors
    import org.apache.spark.ml.linalg._
    val computeDistance = (col1: DenseVector, col2: DenseVector) => {
      var distance = 0.0
      distance = Vectors.sqdist(col1,col2)
      distance
    }

    // register the udf
    val distanceOfTwoVecs = udf(computeDistance)

    // create new column which consists of computed distances. Now we have distances of each iris to its cluster center.
    // Now we can determine who is the closest or farthest to cluster center.
    val data7 = data6.withColumn("distance", distanceOfTwoVecs('clusterCenter,'scaledFeatureVector))
   data7.show()
  }
}