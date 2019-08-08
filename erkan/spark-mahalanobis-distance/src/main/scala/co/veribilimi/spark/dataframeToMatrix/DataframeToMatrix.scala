package co.veribilimi.spark.dataframeToMatrix

import org.apache.spark.ml.linalg.{DenseMatrix => SparkDenseMatrix, DenseVector => SparkDenseVector}
import breeze.linalg.DenseVector
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature.VectorAssembler

object DataframeToMatrix {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[4]")
      .getOrCreate()
    import spark.implicits._

    val mySeq = Seq((1,2,3,4),(5,6,7,8),(9,10,11,12),(13,14,15,16))
    val ds = spark.createDataset(mySeq)
      .selectExpr("_1 as col1","_2 as col2", "_3 as col3", "_4 as col4")

    ds.show()
    val assembler = new VectorAssembler()
      .setInputCols(ds.columns)
      .setOutputCol("features")

    val vectorizedDF = assembler.transform(ds)
    vectorizedDF.printSchema()

    val arrRDD = vectorizedDF.select("features").rdd.map(x => {
      //val dv: Vector = Vectors.zeros(x.size)
      x.size
    })
    println("arrRDD: " + arrRDD)
    arrRDD.foreach(println)


   //val mat:RowMatrix = new RowMatrix(spark.sparkContext.parallelize(Seq((1,2,3),(4,5,6))))
     // arrRDD.foreach(println)

   // val martrixFromDF = SparkDenseMatrix()
    // println(martrixFromDF)

   //val spDV = new SparkDenseMatrix(Array((1, 2, 3),(4,5,6)))
    //println(spDV)


    /************************** Stackoverflow Örnek ******************************/
    val v0 = Vectors.dense(1.0, 0.0, 3.0)
    val v1 = Vectors.sparse(3, Array(1), Array(2.5))
    val v2 = Vectors.sparse(3, Seq((0, 1.5), (1, 1.8)))

    val rows = spark.sparkContext.parallelize(Seq(v0, v1, v2))
    println("rows: " + rows)
    rows.foreach(println)

    val mat: RowMatrix = new RowMatrix(rows)
    println(mat)

    // Get its size.
    val m = mat.numRows()
    val n = mat.numCols()
    /************************** Stackoverflow Örnek ******************************/


    val myRDD = spark.sparkContext.parallelize(Seq((1,2,3),(4,5,6)))
    println("myRDD: " + myRDD)
    myRDD.foreach(println)


  }
}
