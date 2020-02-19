package chapter5

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, column, expr, lit, asc, desc}

object StructuredOperations extends App {

  val spark = SparkSession.builder().appName("Structured-Operations").master("local[*]").getOrCreate()

  spark.sqlContext.sql("set spark.sql.caseSensitive=true")
  import spark.implicits._
//  val x: StructType = df.schema

  val manualSchema = StructType(Array(
    StructField("DEST_COUNTRY_NAME", StringType, nullable = true),
    StructField("ORIGIN_COUNTRY_NAME", StringType, nullable = true),
    StructField("count", LongType, nullable = false, Metadata.fromJson("{\"hello\":\"world\"}"))
  ))

  val df = spark.read
    .format("json")
    .schema(manualSchema)
    .load("/home/ayush/Desktop/extra/Databricks-Spark/src/main/scala/chapter5/2015-summary.json")

  /*val nitin1 = spark.read
    .format("json")
    .option("inferSchema", true)
    .load("/home/ayush/Desktop/extra/Databricks-Spark/src/main/scala/chapter5/nitin1.json")

  val nitin2 = spark.read
    .format("json")
    .option("inferSchema", true)
    .load("/home/ayush/Desktop/extra/Databricks-Spark/src/main/scala/chapter5/nitin2.json")*/

//  val nitinResult = nitin1.join(nitin2, usingColumns = Seq("niti1.id", "niti2.id"), joinType = "left_outer")

//  nitinResult.show
  // both of them are exactly same

  //These two are same.
  (((col("someCol") + 5) * 200) - 6) < col("otherCol")
  expr("(((someCol + 5) * 200) - 6) < otherCol")



  expr("someColumnName")
  column("someColumnName")

  // accessing the dataframe columns
  df.columns.foreach(print(_))

  // accessing the first row
//  print(df.first())

  // creating a row manually
  val myRow = Row("Hello", null, 1, false)

  // accessing data in row
  myRow(0) // type Any
  myRow(0).asInstanceOf[String]

  // select and selectExpr

  df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)
  df.select(expr("DEST_COUNTRY_NAME"), col("DEST_COUNTRY_NAME"), column("DEST_COUNTRY_NAME")).show(2)
  df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)
  df.select(expr("DEST_COUNTRY_NAME AS destination").alias("XYZ")).show(2)

  // short hand is using selectExpr
  df.selectExpr("DEST_COUNTRY_NAME as dest", "DEST_COUNTRY_NAME").show(2)
  df.selectExpr("*", "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry").show(2)

  // Converting to Spark types (literals)
  df.select(expr("*"), lit("hello").as("Greeting")).show(2)

  // Adding Columns
  df.withColumn("Greeting", lit("hello")).show(2)
  df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")).show(2)

  // Renaming Columns
  df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns

  // Removing Columns
  df.drop("ORIGIN_COUNTRY_NAME").columns

  // Changing a Column's Type (cast)
  df.withColumn("count2", col("count").cast("long")).show(2)

  // Filtering rows
  // Both are same
  df.filter(col("count") < 2).show(2)
  df.where("count < 2").show(2)

  df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia")


  // Getting Unique rows
  df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()

  // Union
  import org.apache.spark.sql.Row
  val newRows = Seq(
    Row("New Country", "Other Country", 5L),
    Row("New Country 2", "Other Country 3", 1L)
  )
  val parallelizedRows = spark.sparkContext.parallelize(newRows)
  val newDF = spark.createDataFrame(parallelizedRows, df.schema)
  df.union(newDF)
    .where("count = 1")
    .where($"ORIGIN_COUNTRY_NAME" =!= "United States")
    .show()

  // Sorting Rows
  df.sort("count").show(5)
  df.orderBy("count", "DEST_COUNTRY_NAME").show(5)

  df.orderBy(expr("count desc")).show(2)
  df.orderBy(desc("count"), asc("DEST_COUNTRY_NAME")).show(2)
  df.sortWithinPartitions("count")

  // repartition and coalesce
  df.rdd.getNumPartitions
  df.repartition(5)
  df.repartition(col("DEST_COUNTRY_NAME"))
  df.coalesce(2)

  // Collecting Rows to the Driver
  df.collect()
  df.toLocalIterator()



}

