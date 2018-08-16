package OneHotEncoderEstimator.Example

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    println("Merhaba Spark Scala Projesi")

    val spark = SparkSession.builder
      .master("local")
      .appName("testing")
      .getOrCreate()

    val df = spark.createDataFrame(Seq(
      ("Bir", "İki"),
      ("Bir", "Sıfır"),
      ("İki", "Bir"),
      ("Beş", "İki"),
      ("Dört", "Bir"),
      ("İki", "Üç"),
      ("İki", "Üç"),
      ("Sıfır", "Bir")
    )).toDF("sutun_01", "sutun_02")


    def stringIndexerPipeline(inputCol: String): (Pipeline, String) = {
      val indexer = new StringIndexer().
        setInputCol(inputCol).
        setOutputCol(inputCol + "_indexed")
      val pipeline = new Pipeline().setStages(Array(indexer))
      (pipeline, inputCol + "_indexed")
    }

    val (sutun_01Pipeline, sutun_01_indexed) = stringIndexerPipeline("sutun_01")
    val (sutun_02Pipeline, sutun_02_indexed) = stringIndexerPipeline("sutun_02")



    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("sutun_01_indexed", "sutun_02_indexed"))
      .setOutputCols(Array("sutun_01__indexedVec", "sutun_02__indexedVec"))


    val vectorAssembler = new VectorAssembler().
      setInputCols(encoder.getOutputCols).
      setOutputCol("featureVector")

    val pipeline = new Pipeline().setStages(
      Array(sutun_01Pipeline, sutun_02Pipeline, encoder, vectorAssembler))


    val model = pipeline.fit(df)


    val encoded = model.transform(df)
    encoded.show()
  }
}