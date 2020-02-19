package chapter4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Overview extends App {

  val spark = SparkSession.builder().appName("Overview").master("local[*]").getOrCreate

  val df = spark.range(500).toDF("number")


  /**
    * the following code does not perform addition in Scala, it actually
    * performs addition purely in Spark
     */
  df.select(df.col("number") + 10).show()

  val byteType = ByteType

}
