package com.pari.scala.spark.poc.moneysmart

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
  * @author pari
  * @usecase Data Team Coding Challenge - MoneySmart
  * @version 1.0
  * @since 29/10/2018
  */


object MoneySmart_POC_ETL {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    // create SparkSession context by passing appName & masterName
    val spark = createSparkSession("moneysmart-poc", "local")
    // create my own schema
    val schema = createSchema()


    // create DataFrame by loading input file
    val df = creatDFfromFile(spark, schema, "/tmp/lighthouse-logs.log")

    val newNames = Seq("level", "msg", "path", "process_time_ms", "proxy_reason", "grp", "exper", "dt")
    val dfRenamed = df.toDF(newNames: _*)

    // This import is needed to use the $-notation
    import spark.implicits._
    val dfNullFree = dfRenamed.filter($"level".isNotNull)
    // To extract all the visitor assignment log messages from the log file
    val dfAssigned = dfNullFree.filter($"msg".contains("Request Number is :"))

    import org.apache.spark.sql.functions._
    //dfAssigned.select(col("time"), substring_index(col("msg"), ",", 1).as("user"), substring(col("msg"), col("msg").toString().length - 10, col("msg").toString().length + 4) as ("experiment"), col("msg")).show(truncate = false)
    val tmpDf = dfAssigned.select(col("dt"), substring_index(col("msg"), ",", 1).as("usr"), substring(col("msg"), col("msg").toString().length - 10, col("msg").toString().length + 4) as ("exper"), substring_index(col("msg"), " ", 9).as("grp"), col("msg"))

    val finalDf = tmpDf.select(
      col("dt"),
      substring_index(col("usr"), " : ", -1).as("usr"),
      col("exper"),
      substring_index(col("grp"), " ", -1).as("grp"),
      col("msg"))

    // To create In-memory table
    finalDf.createOrReplaceTempView("log_table")
    // To store in external Database
    storeDB(finalDf, "log_table") //optional

    val q2TmpDF = spark.sql("SELECT \n    exper,\n    dte,\n    MAX(DISTINCT user_group_assignments) AS highest_user_group_assignments\nFROM\n    (SELECT \n        DATE(dt) dte, exper, COUNT(usr) user_group_assignments\n    FROM\n        log_table\n    GROUP BY exper , dte\n    ORDER BY exper , dte , user_group_assignments) AS x\nGROUP BY exper , dte")
    // To create In-memory table
    q2TmpDF.createOrReplaceTempView("q2TmpDF")
    // To store into external Database
    storeDB(q2TmpDF, "q2TmpDF") //optional


    // Q1: A. What are the total number users assigned to the “Test” and “Control” groups in each experiment?
    spark.sql("select exper as Experiment,grp as GroupName,count( distinct usr) Total_Users from log_table group by exper,grp order by exper,grp,Total_Users").show(truncate = false)

    // Q2: B. Which day had the highest number of user group assignments per experiment?
    spark.sql("SELECT \n    dte AS Day,\n    exper AS Experiment,\n    highest_user_group_assignments AS Highest_number_of_user_group_assignments\nFROM\n    q2TmpDF s1\nWHERE\n    highest_user_group_assignments = (SELECT \n            MAX(s2.highest_user_group_assignments)\n        FROM\n            q2TmpDF s2\n        WHERE\n            s1.exper = s2.exper)")
      .show(truncate = false)
  }


  def storeDB(dataFrame: DataFrame, tableName: String): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Overwrite)
      .jdbc("jdbc:mysql://localhost:3306/pari", tableName, getConnection())
  }

  def getConnection(): Properties = {
    Class.forName("com.mysql.jdbc.Driver")
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties
  }

  def creatDFfromFile(spark: SparkSession, schema: StructType, file: String): DataFrame = {
    spark.read
      .option("badRecordsPath", "badRecordsPath")
      .schema(schema)
      .json(file)
  }

  def createSparkSession(appName: String, masterName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .config("spark.master", masterName)
      .getOrCreate()

  }

  def createSchema(): StructType = {
    StructType(
      Array(
        StructField("level", StringType, true),
        StructField("msg", StringType, true),
        StructField("path", StringType, true),
        StructField("process_time_ms", LongType, true),
        StructField("proxy_reason", StringType, true),
        StructField("request_params", StringType, true),
        StructField("requestbackend", StringType, true),
        StructField("time", TimestampType, true)
      )
    )
  }
}
