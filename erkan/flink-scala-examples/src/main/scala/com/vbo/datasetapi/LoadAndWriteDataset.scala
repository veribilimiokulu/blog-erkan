package com.vbo.datasetapi

import org.apache.flink.api.scala._

object LoadAndWriteDataset extends App{

  val env =  ExecutionEnvironment.getExecutionEnvironment

  // Create dataset from file
  val onlineRetail = env.readTextFile("/home/erkan/datasets/OnlineRetail2.csv")

  // How many rows in the dataset?
  println(onlineRetail.count())
// 541910

  // Let's look at the first 5 records
  onlineRetail.first(10).print()
  /**
   * InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
   * 536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,6,2010-12-01 08:26:00,2.55,17850.0,United Kingdom
   * 536365,71053,WHITE METAL LANTERN,6,2010-12-01 08:26:00,3.39,17850.0,United Kingdom
   * 536365,84406B,CREAM CUPID HEARTS COAT HANGER,8,2010-12-01 08:26:00,2.75,17850.0,United Kingdom
   * 536365,84029G,KNITTED UNION FLAG HOT WATER BOTTLE,6,2010-12-01 08:26:00,3.39,17850.0,United Kingdom
   * 536365,84029E,RED WOOLLY HOTTIE WHITE HEART.,6,2010-12-01 08:26:00,3.39,17850.0,United Kingdom
   * 536365,22752,SET 7 BABUSHKA NESTING BOXES,2,2010-12-01 08:26:00,7.65,17850.0,United Kingdom
   * 536365,21730,GLASS STAR FROSTED T-LIGHT HOLDER,6,2010-12-01 08:26:00,4.25,17850.0,United Kingdom
   * 536366,22633,HAND WARMER UNION JACK,6,2010-12-01 08:28:00,1.85,17850.0,United Kingdom
   * 536366,22632,HAND WARMER RED POLKA DOT,6,2010-12-01 08:28:00,1.85,17850.0,United Kingdom
   */

  // Create dataset from array
  val dataSet = env.fromElements("Ali", "Veli",49, 50)
  dataSet.print()
  /**
   * Ali
   * Veli
   * 49
   * 50
   */

  // Split data
  val firstTwoCols = onlineRetail.map(line =>
  {
    val InvoiceNo = line.split(",")(0).trim()
    val StockCode = line.split(",")(1).trim()

    (InvoiceNo, StockCode)
  }
  )

  firstTwoCols.first(5).print()
  /**
   * InvoiceNo,StockCode)
   * (536365,85123A)
   * (536365,71053)
   * (536365,84406B)
   * (536365,84029G)
   */

  // Wite dataset to disk as csv
  firstTwoCols.writeAsCsv("/home/erkan/flink-scala/data_out/fisrtTwo", "|")
  env.execute()
  // will create fisrtTwo folder and write 4 different file named 1,2,3,4

}
