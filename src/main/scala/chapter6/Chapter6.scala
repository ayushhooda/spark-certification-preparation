package chapter6

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, pow, expr, round, bround, corr, monotonically_increasing_id,
  initcap, upper, lower, ltrim, rtrim, trim, lpad, rpad, regexp_extract, regexp_replace,
current_date, current_timestamp, split, explode, udf}

object Chapter6 extends App {

  val spark = SparkSession.builder().appName("chapter-6").master("local[*]").getOrCreate()

  val df = spark.read.format("csv")
    .option("header", value = true)
    .option("inferSchema", value = true)
    .load("/home/ayush/Desktop/extra/Databricks-Spark/src/main/scala/chapter6/2010-12-01.csv")

  df.printSchema()
  df.createOrReplaceTempView("dfTable")

  // converting to spark types
  df.select(lit(5), lit("five"), lit(5.0)).show(5)

  // working with booleans
  df.where(col("InvoiceNo").equalTo(536365))
    .select("InvoiceNo", "Description")
    .show(5, truncate = false)

  // another way
  df.where("InvoiceNo = 536365")
    .select("InvoiceNo", "Description")
    .show(5, truncate = false)

  // multiple filtering
  val priceFilter = col("UnitPrice") > 600
  val descriptionFilter = col("Description").contains("POSTAGE")
  df.where(col("StockCode").isin("DOT")).where(priceFilter or descriptionFilter)
    .show

  val DOTCodeFilter = col("StockCode") === "DOT"
  df.withColumn("isExpensive", DOTCodeFilter and (priceFilter or descriptionFilter))
    .where("isExpensive")
    .select("unitPrice", "isExpensive").show(5)

  // null safe filtering
  df.where(col("Description").eqNullSafe("hello")).show()

  // working with numbers
  val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
  df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)

  // round and bround
  df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)

  // correlation of two columns
  df.stat.corr("Quantity", "UnitPrice")
  df.select(corr("Quantity", "UnitPrice")).show()

  // summary statistics
  df.describe("Quantity", "UnitPrice", "CustomerId").show()

  // adding unique id to each row
  df.select(monotonically_increasing_id()).show(2)

  // working with strings
  df.select(initcap(col("Description"))).show(2, truncate = false)
  df.select(upper(col("Description"))).show(2, truncate = false)
  df.select(lower(col("Description"))).show(2, truncate = false)

  df.select(
    ltrim(lit("      HELLO      ")).as("ltrim"),
    rtrim(lit("    HELLO    ")).as("rtrim"),
    trim(lit("    HELLO    ")).as("trim"),
    lpad(lit("HELLO"), 8, " ").as("lp"),
    rpad(lit("HELLO"), 10, " ").as("rp")).show(2)

  //Note that if lpad or rpad takes a number less than the length of the string, it will always remove
  //values from the right side of the string.

  // Regular Expressions
  val simpleColors = Seq("black", "white", "red", "green", "blue")
  val regexString = simpleColors.map(_.toUpperCase).mkString("|")
  // the | signifies `OR` in regular expression syntax
  df.select(regexp_replace(col("Description"), regexString, "--").alias("color_clean"), col("Description")).show(2)


  // Working with dates and timestamp
  val dateDF = spark.range(10).withColumn("today", current_date())
    .withColumn("now", current_timestamp())

  // Adding date

  // explode
  df.withColumn("splitted", split(col("Description"), " "))
    .withColumn("exploded", explode(col("splitted")))
    .select("Description", "InvoiceNo", "exploded").show(2)

  // UDFs
  val udfExampleDF = spark.range(5).toDF("num")
  def power3(number:Double):Double = number * number * number
  val power3udf = udf(power3(_:Double):Double)
  udfExampleDF.select(power3udf(col("num"))).show()

  spark.udf.register("power3", power3(_:Double):Double)
  udfExampleDF.selectExpr("power3(num)").show(2)



}
