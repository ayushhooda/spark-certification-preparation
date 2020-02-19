package union

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}



object Union extends App {

  // Create the Departments

  val spark = SparkSession
    .builder()
    .appName("DFDS")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  import Data._

  val df1: Dataset[Row] = spark.createDataFrame(departmentsWithEmployeesSeq1).toDF("col0", "col1")
  val df2: DataFrame = spark.createDataFrame(departmentsWithEmployeesSeq2)

  // returns the union of two DFs
  val unionDF = df2.union(df1)
//  val unionDF = df1.union(df2)

//  unionDF.show()

  // write dataframe to a parquet file
//  unionDF.write.parquet("example.parquet")

  // read dataframe from a parquet file
  val dfParquet: Dataset[Row] = spark.read.parquet("example.parquet")

  dfParquet.select(explode($"employees")).show()

  // Making dataframes from various inputs
  /*
  1. Parquet File
  2. CSV File
  3. JSON File
  4. ORC File
  5. Text File
   */
//  df1.show()

}
