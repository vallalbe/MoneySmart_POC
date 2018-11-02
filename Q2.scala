package com.pari.scala.spark.poc.moneysmart

import com.pari.scala.spark.poc.moneysmart.MoneySmart_POC_ETL.createSparkSession
/**
  * @author pari
  * @usecase Data Team Coding Challenge - MoneySmart
  * @version 1.0
  * @since 02/11/2018
  */
object Q2 {
  def main(args: Array[String]): Unit = {
    // create SparkSession context by passing appName & masterName
    val spark = createSparkSession("moneysmart-poc-q2", "local")

    val df = spark.read
      .option("header", true)
      .csv("orders.csv")

    df.createOrReplaceTempView("orders")

    val itempairDF = spark.sql("SELECT a.ProductName AS ProductA, b.ProductName AS ProductB, count(*) as Occurrences\nFROM orders AS a\nINNER JOIN orders AS b ON a.OrderID = b.OrderID\nAND a.ProductName != b.ProductName \nwhere a.ProductName IN (select ProductName from (\nSELECT ProductName, SUM(Quantity) AS TotalQuantity\nFROM orders\nGROUP BY ProductName\nORDER BY TotalQuantity DESC\nLIMIT 10) as b) \nGROUP BY ProductA,ProductB order by Occurrences desc")
    itempairDF.show(truncate = false)
    //itempairDF.write.csv("/Users/parivallalr/IdeaProjects/Test/itempair.csv") //optional




  }

}
