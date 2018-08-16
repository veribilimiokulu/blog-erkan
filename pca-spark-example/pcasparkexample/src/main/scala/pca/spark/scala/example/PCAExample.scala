package pca.spark.scala.example
import org.apache.spark.ml.{PipelineModel, Pipeline}
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler,PCA,PCAModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

object PCAExample {
  def main(args: Array[String]): Unit = {
    //Create spark session
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("ComputeKForPCA")
      .getOrCreate()

    // load iris data
    val data = spark.read.format("csv").option("inferSchema","true").option("header","true")
      .load("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\Datasets\\iris.csv")

    // Check the dataframe
    data.show()

    // The function below returns pipelineModel For
    def ComputeKForPCA(df:DataFrame, k_pca:Int): PipelineModel = {

      // Pick up input columns
      val inputColumns = data.columns.filter(_!="Species")

      val vectorAssembler = new VectorAssembler().
        setInputCols(inputColumns).
        setOutputCol("featureVector")

      val standartScaler = new StandardScaler()
        .setInputCol("featureVector")
        .setOutputCol("scaledFeatureVector")
        .setWithStd(true)
        .setWithMean(false)

     val pca = new PCA()
       .setInputCol("scaledFeatureVector")
       .setOutputCol("PCAScaledFeatureVector")
       .setK(k_pca)

      val pipeline = new Pipeline().setStages(
        Array(vectorAssembler, standartScaler, pca))
      pipeline.fit(df)

    }
    //end of function

    // Finding optimal k number

    for(k_pca <- 2 to 4 by 1){
      val pipelineModel =  ComputeKForPCA(data,k_pca)

      val transformedDF = pipelineModel.transform(data)
      val pcaModel = pipelineModel.stages.last.asInstanceOf[PCAModel]
      val explainedVariance = pcaModel.explainedVariance
      println("k value:"+k_pca, "Explained Variance: "+explainedVariance, explainedVariance.toArray.sum)
    }




  }
}
