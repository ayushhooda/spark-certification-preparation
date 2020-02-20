package chapter7

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Chapter7 extends App {

  val spark = SparkSession.builder().appName("chapter-7").master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/home/ayush/Desktop/extra/Databricks-Spark/src/main/scala/chapter7/2010-12-01.csv")
    .coalesce(5)

  df.cache()

  df.createOrReplaceTempView("dfTable")

  print(df.count())

  df.select(count("StockCode")).show()
  df.select(countDistinct("StockCode")).show()
  df.select(approx_count_distinct("StockCode", 0.1)).show()
  df.select(first("StockCode"), last("StockCode")).show()
  df.select(min("Quantity"), max("Quantity")).show()
  df.select(sum("Quantity")).show()
  df.select(sumDistinct("Quantity")).show()
  df.select(avg("Quantity")).show()
  df.select(mean("Quantity")).show()
  df.select(var_pop("Quantity"), var_samp("Quantity"),
    stddev_pop("Quantity"), stddev_samp("Quantity")).show()
  df.select(collect_set("Country"), collect_list("Country")).show()

  // grouping
  df.groupBy("InvoiceNo", "CustomerId").count().show()
  df.groupBy("InvoiceNo").agg("Quantity"->"avg", "Quantity"->"stddev_pop").show()

  // window function
  //  Spark supports three kinds of window functions: ranking functions, analytic functions, and aggregate functions.


}
