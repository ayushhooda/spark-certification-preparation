package explode

import utilities.SparkUtilites

import org.apache.spark.sql.functions._

/*
There are four types of explode functions:
1. explode
2. explode_outer
3. posexplode
4. posexplode_outer
 */

object Explode extends App {

  val spark = SparkUtilites.getSparkSession("ExplodeExamples")

  import spark.implicits._

  val df = spark.createDataFrame(Data.data)

  // Creates row for every record in array but don't create if array is empty or null
  val explodeDF = df.select($"name", explode($"properties"))

  // Creates row for every record in array even for empty array
  val explodeOuterDF = df.select($"name", explode_outer($"properties"))

  val posExplodeDF = df.select($"name", posexplode($"properties"))

  val posExplodeOuterDF = df.select($"name", posexplode_outer($"properties"))

  explodeOuterDF.show()

}
