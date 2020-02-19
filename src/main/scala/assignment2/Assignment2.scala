package assignment2

import org.apache.spark.sql.SparkSession

object Assignment2 {

  def main(args: Array[String]): Unit = {

    // Creating Spark Session
    val sparkSession = SparkSession
      .builder()
      .appName("Assignment-2")
      .master("local")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    val stationDS = sparkSession.sqlContext.read
      .format("csv").option(key = "header", value = true)
      .option(key = "inferSchema", value = true)
      .load("/home/ayush/Desktop/extra/Databricks-Spark/src/main/scala/assignment2/201508_station_data.csv")
//      .schema(Station.stationSchema)
//      .as[Station]

    val tripDS = sparkSession.sqlContext.read
      .format("csv").option(key = "header", value = true)
//      .schema(Trip.tripSchema)
      .option(key = "inferSchema", value = true)
      .load("/home/ayush/Desktop/extra/Databricks-Spark/src/main/scala/assignment2/201508_trip_data.csv")
      .withColumnRenamed("Start Station", "Source")
      .withColumnRenamed("End Station", "Destination")
//      .as[Trip]

    print(stationDS.count)
    print(tripDS.schema)

  }

}
