package co.veribilimi.spark.ml.squared.distance.vector.KMeans

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.{Column, DataFrame, SparkSession, UDFRegistration}
import org.apache.spark.sql.functions._



object KMeans {
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

      // create a vectorAssembler put all those inputs. Creates a new column vector named featureVector
      val vectorAssembler = new VectorAssembler().
        setInputCols(inputColumns).
        setOutputCol("featureVector")

      // create a scaler and scale all inputs. Gets in what vectorAssembler outs and creates new column as a vector but scaled one
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
    //end of the function

    /**** Finding optimal k number *********/

      //Create an evaluator object
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

      // get evaluator score as double. The dataframe must have scaledFeatureVector column which we declared while
      // creating the evaluator object
      val score = evaluator.evaluate(transformedDF)
      val kmeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]
      println("k value:"+k, "Score: "+score, "KMeans ComputeCost: "+ kmeansModel.computeCost(transformedDF))
    }

    // We choose 3 as the optimal cluster number
    val pipelineModel =  ComputeClusteringModel(data5,3)

    // Pick up clustering model from pipeline stages. It is one of them. Last one.
    val kmeanModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]

    // transform pipeline model and get new computed features like cluster number.
    // We can see which iris is located in which cluster
    val transformedDF = pipelineModel.transform(data5)

    /********** COMPUTE THE DISTANCE OF EACH IRIS FLOWER TOWARDS ITS CLUSTER CENTER EUCLIDIAN DISTANCE *********/

    // get cluster center from clustering model which is a vector (think it as coordinates)
    val clusterCenters = kmeanModel.clusterCenters

    import spark.implicits._

    // we match cluster centers with its cluster number.
    val vec = clusterCenters.zipWithIndex

    // array to seq
    val vecDF = vec.toSeq

    // seq to df
    val seqDF = vecDF.toDS

    //check df if it is ok
    seqDF.show()

    // join newly created cluster center and cluster number dataframe with the transformed one from the original.
    // be aware you must use left join. transformed (big one) must be on the left
    val data6 = transformedDF.join(seqDF, transformedDF("cluster") === seqDF("_2"), "left")
      .withColumnRenamed("_1","clusterCenter")
      .drop("_2")


    // Create a user defined function which computes distance two vectors
    import org.apache.spark.ml.linalg._
    val computeDistance = (col1: DenseVector, col2: DenseVector) => {
      var distance = 0.0
      distance = Vectors.sqdist(col1,col2)
      distance
    }

    // register the udf
    val distanceOfTwoVecs = udf(computeDistance)

    // Create a new column which consists of computed distances. Now we have distances of each iris to its cluster center.
    // Now we can determine who is the closest or farthest to cluster center.
    val data7 = data6.withColumn("distance", distanceOfTwoVecs('clusterCenter,'scaledFeatureVector))
    data7.show()

  /******************* FIND Z SCORES FOR OUTLIER SCORE **************/
    // Determine outlier in each cluster; the farther, the outlier
    val data8 = data7.groupBy("cluster").agg(stddev('distance).as("clusterStd"),
      min('distance).as("minOfCluster"), max('distance).as("maxOfCluster"),mean('distance).as("meanOfCluster"))
    data8.show()

    // join old df with last one. Left join
    val data9 = data7.join(data8, data7.col("cluster") === data8.col("cluster"), "left")
      .drop(data8.col("cluster"))
    data9.show()

    // comute z value for every iris (row) abs((distance - mean))/std
    val data10 = data9.withColumn("z_score", (abs(('distance - 'meanOfCluster))/'clusterStd))
    data10.show()

    // Filter out outliers where z_score > 3. In other words where further than 3 std
    val data11 = data10.withColumn("outlier", (when(col("z_score").gt(3.0),"outlier")
      .otherwise("normal")))
    data11.where(col("outlier").equalTo("outlier")).show()

  }
}

