package chapter9

import org.apache.spark.sql.SparkSession

object Chapter9 extends App {

  val spark = SparkSession.builder().appName("chapter-9").master("local[*]").getOrCreate()

  // Spark's Core Data Sources
  // 2. JSON
  // 3. Parquet
  // 4. ORC
  // 5. JDBC/ODBC connections
  // 6. Plain-text files


  // Read Modes
  // permissive(default),dropMalformed, failFast

  // Write Modes
  // append, overwrite, errorIfExists(default), ignore
  spark.read

  // 1. CSV
  val df1 = spark.read.format("csv")
    .option("header", "true")
    .option("mode", "FAILFAST")
    .option("inferSchema", "true")
    .load("some/path/to/file.csv")

  df1.write.format("csv").mode("overwrite").option("sep", ",")
    .save("somepath")

  // 2. JSON
  val df2 = spark.read.format("json").option("mode", "FAILFAST").option("inferSchema", "true")
    .load("/data/flight-data/json/2010-summary.json")

  df2.write.format("json").mode("overwrite").save("path")

  // 3. Parquet

  // 4. ORC

  // 5. Databases
  // Todo



}
