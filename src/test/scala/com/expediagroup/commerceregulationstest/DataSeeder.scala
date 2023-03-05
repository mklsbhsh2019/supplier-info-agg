package com.expediagroup.commerceregulationstest

import org.apache.spark.sql.SparkSession

object DataSeeder extends App{
  val spark = SparkSession.builder.appName("Supplier Info Agg")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", s"file:///${System.getProperty("user.dir")}/spark-warehouse")
    .enableHiveSupport.getOrCreate

  spark.sql("DROP TABLE property_fact")
  spark.sql("CREATE TABLE property_fact (property_id STRING," +
    "booking_date DATE, cost DECIMAL(10,2)," +
    "fees DECIMAL(10,2), taxes DECIMAL(10,2))" +
    "row format delimited fields terminated by ','")
  spark.sql(s"LOAD DATA LOCAL INPATH '${getClass.getResource("/PropertyFactSeedData.csv").getPath}' INTO TABLE property_fact")

  spark.sql("DROP TABLE property_dim")
  spark.sql("CREATE TABLE property_dim (property_id STRING, " +
    "property_name STRING, " +
    "property_address STRING) " +
    "row format delimited fields terminated by ','")
  spark.sql(s"LOAD DATA LOCAL INPATH '${getClass.getResource("/PropertyDimSeedData.csv").getPath}' INTO TABLE property_dim")
}
