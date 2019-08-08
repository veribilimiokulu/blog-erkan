
import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.SparkSession
object ReadFromParquet {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("ReadFromParquet")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    // read from csv
    val df = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .option("sep",";")
      .load("D:\\Datasets\\OnlineRetail.csv")

    // show csv read data
    df.show()


    // save df as parquet
    df // df to write
      .coalesce(1) // as a 1 partition not many
      .write // write
      .mode("overwrite")  // do I overwrite: yes
      .parquet("D:\\Datasets\\OnlineRetailParquet") // where to write (need to write permission)

    // read from parquet
    val df_parquet = spark.read.format("parquet")
      .load("D:\\Datasets\\OnlineRetailParquet")

    // show parquet read data
    df_parquet.show()

  }
}
