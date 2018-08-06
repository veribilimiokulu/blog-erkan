package confusion.pck
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler,StandardScaler}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.{PipelineModel, Pipeline}
import org.apache.spark.ml.classification.{DecisionTreeClassifier,DecisionTreeClassificationModel}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.tuning.{ParamGridBuilder,TrainValidationSplit,TrainValidationSplitModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import scala.util.Random

object Main {
  def main(args: Array[String]): Unit = {

    // Spark session oluşturma
    val spark = SparkSession.builder
      .master("local")
      .appName("confusion")
      .getOrCreate()

    // Veri okuma ve dataframe oluşturma
    val data = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\Datasets\\iris.csv")

    // dataframe göz atma
    println(data.show(20))


    // Null değer kontrolü
    val colNames = data.columns
    var sayac = 1
    for (col <- colNames){
      if(data.filter(data.col(col).isNull).count() > 0){
        println(sayac+". "+col + " var.")
      }else{
        println(sayac+". "+col + " yok.")
      }
      sayac+=1
    }


    //Veri setini böl
    val Array(trainData, testData) = data.randomSplit(Array(0.8, 0.2),seed = 142)


    // Sınıflandırıcı (Karar Ağacı) Oluştur
    def ClassifyMe(df:DataFrame): PipelineModel = {

      // Analize girecek sütunları toplayalım
      val inputColumns = Array("SepalLengthCm","SepalWidthCm","PetalLengthCm","PetalWidthCm")

      // VectorAssembler nesnesi oluştur
      val vectorAssembler = new VectorAssembler().
        setInputCols(inputColumns).
        setOutputCol("featureVector")

      // StandardScaler oluştur
      val standartScaler = new StandardScaler()
        .setInputCol("featureVector")
        .setOutputCol("features")
        .setWithStd(true)
        .setWithMean(false)

      // Hedef değişkeni indeksle
      val labelIndexer = new StringIndexer().
        setHandleInvalid("skip").
        setInputCol("Species").
        setOutputCol("label")

      // Sınıflandırıcı oluştur
      val classifier = new DecisionTreeClassifier().
        setFeaturesCol("features").
        setLabelCol("label").
        setSeed(142)

      // Pipeline oluştur
      val pipeline = new Pipeline().setStages(
        Array(vectorAssembler, standartScaler,labelIndexer, classifier))

      // TrainValidationSplit'i eğit ve TrainValidationSplitModel döndür
      pipeline.fit(df)

    }

    // Modeli eğit karşılığında PielineModel dönecek
    val pipelineModel = ClassifyMe(trainData)



    // PipelineModel'den DecisionTreeClassificationModel'e
    val dtModel = pipelineModel.stages.last.asInstanceOf[DecisionTreeClassificationModel]

    // PipelineModeli Transform Et: Karşılığında rawPrediction, probability, prediction
    val trsDF = pipelineModel.transform(testData)

    // Hata Matrisini dataframe ile yazdırma
    val confusionMatrix = trsDF.groupBy("label")
      .pivot("prediction", (0 to 2))
      .count().na.fill(0.0).orderBy("label")
      confusionMatrix.show()

    // Hata matrisini eski kütüphane yazdırma ve accuracy bulma
    import org.apache.spark.mllib.evaluation.MulticlassMetrics
    import spark.implicits._

    // predictions dataframeden hedef niteliği ve tahmin sonuçların seçi, ikisini de double türüne çevirip rdd olarak predictionRDD'ye atıyoruz.
    val predictionRDD = trsDF.select("prediction","label").as[(Double, Double)].rdd

    // MulticlassMetrics sınıfından bir nesne yaratıyoruz. Parametre olarak yeni oluşturduğumuz rdd'yi veriyoruz. Bu bize hata matrainini de bir özellik olarak içinde barındıran bir nesne verecek.
    val multiclassMetrics = new MulticlassMetrics(predictionRDD)

    // hata matrisini gösterelim
    println(multiclassMetrics.confusionMatrix)
    println(multiclassMetrics.accuracy)
  }
}
