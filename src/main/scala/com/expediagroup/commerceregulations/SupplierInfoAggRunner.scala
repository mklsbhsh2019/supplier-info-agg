package com.expediagroup.commerceregulations

import java.io.File

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._


object SupplierInfoAggRunner extends App {

  val columnMappingPath = args(0)
  val startDate = args(1)
  val endDate = args(2)
  val columnMappingReader = YAMLMapper.builder().addModule(DefaultScalaModule).build()
  val columnMapping = columnMappingReader.readValue(new File(columnMappingPath), classOf[ColumnMapping])

  val spark = SparkSession.builder.appName("Supplier Info Agg")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", s"file:///${System.getProperty("user.dir")}/spark-warehouse")
    .enableHiveSupport.getOrCreate

  val factAggDf = spark.sql(s"select ${columnMapping.factTable.columnMap("supplier_id")} as supplier_id, " +
    s"${columnMapping.factTable.columnMap("quarter_of_the_year")} as quarter_of_the_year, " +
    s"${columnMapping.factTable.columnMap("total_cost")} as total_cost, " +
    s"${columnMapping.factTable.columnMap("total_fees")} as total_fees, " +
    s"${columnMapping.factTable.columnMap("total_taxes")} as total_taxes " +
    s"from ${columnMapping.factTable.tableName} " +
    s"where ${columnMapping.factTable.dateColumn} >= to_date('${startDate}') and ${columnMapping.factTable.dateColumn} <= to_date('${endDate}')" +
    s"group by 1,2")

  val dimDf = spark.sql(s"select ${columnMapping.factTable.columnMap("supplier_id")} as supplier_id, " +
    s"${columnMapping.dimTable.columnMap("supplier_name")} as supplier_name, " +
    s"${columnMapping.dimTable.columnMap("supplier_address")} as supplier_address " +
    s"from ${columnMapping.dimTable.tableName} ")

  factAggDf.join(dimDf, "supplier_id")
    .write.mode(SaveMode.Overwrite)
    .saveAsTable("supplier_info")

}
