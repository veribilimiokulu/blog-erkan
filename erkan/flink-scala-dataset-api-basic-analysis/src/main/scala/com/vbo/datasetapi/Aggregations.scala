package com.vbo.datasetapi
import org.apache.flink.api.scala._
import java.sql.Timestamp
import org.apache.flink.api.common.operators.Order


object Aggregations extends App {

  val env = ExecutionEnvironment.getExecutionEnvironment

  /**************************************************  PRODUCTS   ****************************************/
  // Define Products schema
  case class Products(productId: Int,
                       productCategoryId: Int,
                       productName: String,
                       productDescription: String,
                       productPrice: Double,
                       productImage: String)

  // Create dataset from file
  // I had to run sed -i 's+""++g' retail_db/products2.csv this before reading
  val productsDS = env.readCsvFile[Products]("/home/erkan/datasets/retail_db/products2.csv",
    fieldDelimiter=",", ignoreFirstLine = true, quoteCharacter = '"')

  println(productsDS.count())
  // 1345

  productsDS.first(5).print()
  /*
  Products(1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy)
  Products(2,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat)
  Products(3,2,Under Armour Men's Renegade D Mid Football Cl,,89.99,http://images.acmesports.sports/Under+Armour+Men%27s+Renegade+D+Mid+Football+Cleat)
  Products(4,2,Under Armour Men's Renegade D Mid Football Cl,,89.99,http://images.acmesports.sports/Under+Armour+Men%27s+Renegade+D+Mid+Football+Cleat)
  Products(5,2,Riddell Youth Revolution Speed Custom Footbal,,199.99,http://images.acmesports.sports/Riddell+Youth+Revolution+Speed+Custom+Football+Helmet)
   */



  /**************************************************  ORDERS   ****************************************/
  // Define Orders schema
  case class Orders(orderId: Int,
                    orderDate: Timestamp,
                    orderCustomerId: Int,
                    orderStatus: String)

  // Create dataset from file
  val ordersDS = env.readCsvFile[Orders]("/home/erkan/datasets/retail_db/orders.csv",
    fieldDelimiter=",", ignoreFirstLine = true)

  println(ordersDS.count())
  // 68883

  ordersDS.first(5).print()
  /**
   * Orders(51730,2014-06-14 00:00:00.0,2153,COMPLETE)
   * Orders(51731,2014-06-14 00:00:00.0,8895,PROCESSING)
   * Orders(51732,2014-06-14 00:00:00.0,10222,PENDING)
   * Orders(51733,2014-06-14 00:00:00.0,4078,CANCELED)
   * Orders(51734,2014-06-14 00:00:00.0,6522,PENDING_PAYMENT)
   */

  // GroupBy orderStatus.
  ordersDS.map(x=>(x.orderStatus, x.orderId))
    .groupBy(0).sum(1).first(25).print()
  /*
  (PROCESSING,286158867)
  (PAYMENT_REVIEW,25126996)
  (PENDING,264303939)
  (PENDING_PAYMENT,516200083)
  (CANCELED,49540191)
  (COMPLETE,789600262)
  (ON_HOLD,130845249)
  (SUSPECTED_FRAUD,53876492)
  (CLOSED,256816207)
   */

  /**************************************************  ORDER ITEMS   ****************************************/
  // Define Order Items Schema
  case class OrderItems(orderItemName: Int,
                        orderItemOrderId: Int,
                        orderItemProductId: Int,
                        orderItemQuantity: Int,
                        orderItemSubTotal: Double,
                        orderItemProductPrice: Double)
  val orderItemsDS = env.readCsvFile[OrderItems](filePath = "/home/erkan/datasets/retail_db/order_items.csv",
    fieldDelimiter = ",", ignoreFirstLine = true)

  println(orderItemsDS.count())
  // 172198

  orderItemsDS.first(5).print()
  /**
   * OrderItems(1,1,957,1,299.98,299.98)
   * OrderItems(2,2,1073,1,199.99,199.99)
   * OrderItems(3,2,502,5,250.0,50.0)
   * OrderItems(4,2,403,1,129.99,129.99)
   * OrderItems(5,4,897,2,49.98,24.99)
   */

  /************************************************** JOIN ORDESR and ORDER ITEMS   ****************************************/
    // where refers left dataset (ordersDS) orderId, equalTo refers right dataset (orderItemsDS) orderItemOrderId
  val ordersJoinedDS = ordersDS.join(orderItemsDS).where(0).equalTo(1)

  ordersJoinedDS.first(5).print()
  /**
   * (Orders(35211,2014-02-27 00:00:00.0,4849,COMPLETE),OrderItems(87944,35211,365,4,239.96,59.99))
   * (Orders(35214,2014-02-27 00:00:00.0,388,PENDING_PAYMENT),OrderItems(87954,35214,403,1,129.99,129.99))
   * (Orders(35214,2014-02-27 00:00:00.0,388,PENDING_PAYMENT),OrderItems(87955,35214,1014,3,149.94,49.98))
   * (Orders(35214,2014-02-27 00:00:00.0,388,PENDING_PAYMENT),OrderItems(87956,35214,1004,1,399.98,399.98))
   * (Orders(35214,2014-02-27 00:00:00.0,388,PENDING_PAYMENT),OrderItems(87957,35214,365,4,239.96,59.99))
   */
  println(ordersJoinedDS.count())
  // 172198


  /********************************  Find most selling products in terms of total amount   *****************************/
  // Join ordersJoinedDS and products
  val ordersWithProductsDS = ordersJoinedDS.join(productsDS).where(_._2.orderItemProductId).equalTo(_.productId)

  println(ordersWithProductsDS.count())
  // 172198

  ordersWithProductsDS.first(5).print()
  //
  /*
  ((Orders(53282,2014-06-25 00:00:00.0,4285,PENDING_PAYMENT),OrderItems(133137,53282,37,2,69.98,34.99)),Products(37,3,adidas Kids' F5 Messi FG Soccer Cleat,,34.99,http://images.acmesports.sports/adidas+Kids%27+F5+Messi+FG+Soccer+Cleat))
  ((Orders(53364,2014-06-26 00:00:00.0,9001,COMPLETE),OrderItems(133333,53364,37,1,34.99,34.99)),Products(37,3,adidas Kids' F5 Messi FG Soccer Cleat,,34.99,http://images.acmesports.sports/adidas+Kids%27+F5+Messi+FG+Soccer+Cleat))
  ((Orders(53539,2014-06-27 00:00:00.0,1499,PROCESSING),OrderItems(133801,53539,37,5,174.95,34.99)),Products(37,3,adidas Kids' F5 Messi FG Soccer Cleat,,34.99,http://images.acmesports.sports/adidas+Kids%27+F5+Messi+FG+Soccer+Cleat))
  ((Orders(21,2013-07-25 00:00:00.0,2711,PENDING),OrderItems(66,21,37,2,69.98,34.99)),Products(37,3,adidas Kids' F5 Messi FG Soccer Cleat,,34.99,http://images.acmesports.sports/adidas+Kids%27+F5+Messi+FG+Soccer+Cleat))
  ((Orders(36176,2014-03-04 00:00:00.0,9331,PENDING),OrderItems(90342,36176,37,2,69.98,34.99)),Products(37,3,adidas Kids' F5 Messi FG Soccer Cleat,,34.99,http://images.acmesports.sports/adidas+Kids%27+F5+Messi+FG+Soccer+Cleat))
   */

  // Find most selling products in terms of total amount
  ordersWithProductsDS.map(x => (x._2.productName, x._1._1.orderStatus, x._1._2.orderItemSubTotal))
    .filter(_._2.contains("CANCELED"))
    .groupBy(0)
    .sum(2)
    .sortPartition(2, Order.DESCENDING).setParallelism(1)
    .first(10).print()
    /*
    (Field & Stream Sportsman 16 Gun Fire Safe,CANCELED,134393.27999999965)
    (Perfect Fitness Perfect Rip Deck,CANCELED,85785.70000000008)
    (Nike Men's Free 5.0+ Running Shoe,CANCELED,80691.9300000001)
    (Diamondback Women's Serene Classic Comfort Bi,CANCELED,80094.6600000001)
    (Pelican Sunstream 100 Kayak,CANCELED,66196.6899999998)
    (Nike Men's Dri-FIT Victory Golf Polo,CANCELED,65750.0)
    (Nike Men's CJ Elite 2 TD Football Cleat,CANCELED,60705.32999999974)
    (O'Brien Men's Neoprene Life Vest,CANCELED,58126.74000000006)
    (Under Armour Girls' Toddler Spine Surge Runni,CANCELED,26153.459999999992)
    (LIJA Women's Eyelet Sleeveless Golf Polo,CANCELED,2145.0)
     */




}



