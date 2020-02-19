package chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object StructuredStreaming extends App {

  val spark = SparkSession
    .builder
    .appName("Structured-Streaming")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  // Reading data as batch
  val staticDataFrame = spark.read.format("csv")
    .option("header", value = true)
    .option("inferSchema", value = true)
    .load("/home/ayush/Desktop/extra/Databricks-Spark/src/main/scala/chapter3/*.csv")

  val staticSchema = staticDataFrame.schema

  staticDataFrame.selectExpr("CustomerId", "(UnitPrice * Quantity) as total_cost", "InvoiceDate")
    .groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day"))
    .sum("total_cost").withColumnRenamed("sum(total_cost)", "total_expense")
    .where("CustomerId = '13069.0'")
//    .show()

  // Reading data as streaming
  val streamingDataFrame = spark.readStream
    .schema(staticSchema)
    .option("maxFilesPerTrigger", value = 1)
    .format("csv")
    .option("header", "true")
    .load("/home/ayush/Desktop/extra/Databricks-Spark/src/main/scala/chapter3/*.csv")


  // See whether our dataFrame is streaming or not

  print(streamingDataFrame.isStreaming)

  import spark.implicits._

  // col, column, $, ' (All can be used interchangeably)
  val streamingDataQuery = streamingDataFrame.selectExpr("CustomerId", "(UnitPrice * Quantity) as total_cost", "InvoiceDate")
    .groupBy($"CustomerId", window($"InvoiceDate", "1 day"))
    .sum("total_cost").withColumnRenamed("sum(total_cost)", "total_cost")

  /*streamingDataQuery.writeStream
    .format("memory")
    .queryName("customer_purchases") // name of in-memory table
    .outputMode("complete")
    .start
*/
  streamingDataQuery
    .writeStream
    .format("memory")
    .queryName("customer_purchases")
    .outputMode("complete")
    .start
//    .awaitTermination(5000)
  Thread.sleep(50000)

  print("?????")

  // Now you can query against this queryName specified above to debug our result.
  spark.sql("""select * from customer_purchases order by total_cost desc""")
    .show(5)

//Thread.sleep(50000)

}
