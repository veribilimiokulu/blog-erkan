package pcaPck
import org.apache.spark.ml.{PipelineModel, Pipeline}
import org.apache.spark.ml.feature.{OneHotEncoderEstimator,VectorAssembler, StandardScaler,PCA,PCAModel,StringIndexer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}

object PCAExampleAdult {
  def main(args: Array[String]): Unit = {
    //Create spark session
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("ComputeKForPCA")
      .getOrCreate()

    // load iris data
    val data = spark.read.format("csv").option("inferSchema", "true").option("header", "true")
      .load("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\Datasets\\adult_dataset\\adult.data")

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


      // PCA modelin de içinde olduğu bir pipeline hazırla
        def DetermineLDANumber(df:DataFrame, k_pca:Int): PipelineModel = {

          val (workclassPipeline, workclass_indexed) = stringIndexerPipeline("workclass")
          val (educationPipeline, education_indexed) = stringIndexerPipeline("education")
          val (education_numPipeline, education_num_indexed) = stringIndexerPipeline("education_num")
          val (marital_statusPipeline, marital_status_indexed) = stringIndexerPipeline("marital_status")
          val (occupationPipeline, occupation_indexed) = stringIndexerPipeline("occupation")
          val (relationshipPipeline, relationship_indexed) = stringIndexerPipeline("relationship")
          val (racePipeline, race_indexed) = stringIndexerPipeline("race")
          val (sexPipeline, sex_indexed) = stringIndexerPipeline("sex")
          val (native_countryPipeline, native_country_indexed) = stringIndexerPipeline("native_country")


          // StringIndexer dan çıkanları OneHotEstimator a sokalım
          val oneHotEncoder = new OneHotEncoderEstimator()
            //.setDropLast(true)
            //.setHandleInvalid("skip")
            .setInputCols(Array("workclass_indexed","education_indexed", "education_num_indexed","marital_status_indexed","occupation_indexed","relationship_indexed",
            "race_indexed","sex_indexed","native_country_indexed"))
            .setOutputCols(Array("workclass_indexedVec","education_indexedVec", "education_num_indexedVec","marital_status_indexedVec","occupation_indexedVec","relationship_indexedVec",
              "race_indexedVec","sex_indexedVec","native_country_indexedVec"))


          // Yukarıda yazdığımız oneHotPipeline() fonksiyonu ile her bir kategorik nitelik için bir pipeline nesnesini ve ilgili
          // sütun ismini alıp bir değişkende tutalım. Her kategorik nitelik için ayrı ayrı yapıyoruz.

          // Analize girecek sütunları toplayalım
          val OHECols = oneHotEncoder.getOutputCols ++ Array("age","fnlwgt","capital_gain","capital_loss","hours_per_week")


          // Model için gerekli nitelikleri seçmek:
          // Orijinal dataframe'den kategorik nitelikleri ve hedef değişken olan label sütununu çıkarıp
          // yeni oluşturduğumuz vector türündeki isimleri ekliyoruz.
          // Buradaki hazırlığın amacı vector assembler için vereceğimiz sütun isimlerini bir arada toplamaktır.
          val vectorAssembler = new VectorAssembler().
            setInputCols(OHECols).
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
            Array(workclassPipeline, educationPipeline, education_numPipeline,marital_statusPipeline,occupationPipeline,relationshipPipeline,
              racePipeline,sexPipeline, native_countryPipeline, oneHotEncoder, vectorAssembler, standartScaler, pca))
          pipeline.fit(df)

        }
        //end of function

        // Finding optimal k number
        val exvar = Array[Double]()
        for (k_pca <- 2 to data.columns.length by 1) {

          val pipelineModel = DetermineLDANumber(data, k_pca)

          val transformedDF = pipelineModel.transform(data)
          val pcaModel = pipelineModel.stages.last.asInstanceOf[PCAModel]
          val explainedVariance = pcaModel.explainedVariance
          println("k value:" + k_pca, "Explained Variance: " + explainedVariance, explainedVariance.toArray.sum)
          exvar:+explainedVariance.toArray.sum
        }
        exvar.foreach(println)
  }
}
