package chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object FlightData extends App {

  val spark = SparkSession.builder().master("local[*]").appName("FlightData").getOrCreate()

  spark.conf.set("spark.sql.shuffle.partitions", "5")

  spark.sparkContext.setLogLevel("ERROR")

  // Read data from csv file
  val flightData2015 = spark
    .read
    .option("inferSchema", value = true)
    .option("header", value = true)
    .csv("/home/ayush/Desktop/extra/Databricks-Spark/src/main/scala/chapter2/flight-data.csv")

//  print(flightData2015.sort("count").explain())

  // Register dataframe as view to be used by sql queries.
  // DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count
  flightData2015.createOrReplaceTempView("flight_Data_2015")

  val sqlWay = spark
    .sql("Select DEST_COUNTRY_NAME, count(1) from flight_Data_2015 group by DEST_COUNTRY_NAME")

  val dfWay = flightData2015.groupBy("DEST_COUNTRY_NAME").count().sort(desc("count"))

  // maximum number of flights to and from any given location
  flightData2015.select(max("count"))

  // find the top five destination countries in the data
  flightData2015
    .groupBy("DEST_COUNTRY_NAME")
    .sum("count")
    .sort(desc("sum(count)"))
    .limit(5)

  spark.sql("select DEST_COUNTRY_NAME, sum(count) from flight_Data_2015 group by DEST_COUNTRY_NAME order by sum(count) desc limit 5")
    .show()

}
