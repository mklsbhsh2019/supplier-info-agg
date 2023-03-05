package com.expediagroup.commerceregulationstest

import org.apache.spark.sql.SparkSession

object DataChecker extends App{
  val spark = SparkSession.builder.appName("Supplier Info Agg")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", s"file:///${System.getProperty("user.dir")}/spark-warehouse")
    .enableHiveSupport.getOrCreate

  spark.sql("select * from supplier_info").show
}
