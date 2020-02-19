package hadoopexamass1

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, Row, SparkSession}


object Assessment1 extends App {

  val spark = SparkSession.builder.appName("assessment-1").master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  // Create a DF from sequence of objects
  val df1 = Seq(("1001", "Hadoop", 7000),
    ("1002", "Spark", 7000),
    ("1003", "Cassandra", 7000),
    ("1004", "Python", 7000)).toDF("course_id", "course_name", "course_fee")

  // Create a case class Learner with fields name, email and city
  case class Learner(name: String, email: String, city: String)

  // Create 7 learner objects
  val learners = Seq(Learner("Amit", "amit@hadoopexam.com", "Mumbai"),
  Learner("Rakesh", "rakesh@hadoopexam.com", "Pune"),
  Learner("Jonathan", "jonathan@hadoopexam.com", "NewYork"),
  Learner("Michael", "michael@hadoopexam.com", "Washington"),
  Learner("Simon", "simon@hadoopexam.com", "HongKong"),
  Learner("Venkat", "venkat@hadoopexam.com", "Chennai"),
  Learner("Roshni", "roshni@hadoopexam.com", "anglore"))

  // Create DF using above learner objects
  val df2 = spark.createDataFrame(learners)

  // Create and Encoder for learner case class
  val learnerEncoder = Encoders.product[Learner]

  // Fetch the schema from the Encoder
  learnerEncoder.schema

  // convert the above df to ds
  val ds2 = df2.as[Learner]

  // store the df as parquet file (filename should be hadoopexam-df-exercise2.parquet)
  df2.write.mode("overwrite").parquet("hadoopexam-df-exercise2.parquet")

  // read the same file
  val readFromFile = spark.read.parquet("hadoopexam-df-exercise2.parquet")
  val readFromFile2 = spark.read.format("parquet").load("hadoopexam-df-exercise2.parquet")

  // show the read data
  readFromFile.show()

  // Define a case class by name HECouser
  case class HECouser(id: Int, name: String, fee: Int, venue: String, duration: Int)

  // create a ds with 5 objects of HECouser
  val objs = Seq(
    HECouser(1, "Hadoop", 6000, "Mumbai", 5),
    HECouser(2, "Spark", 5000, "Pune", 4),
    HECouser(3, "Python", 4000, "Hyderabad", 3),
    HECouser(4, "Scala", 4000, "Kolkata", 3),
    HECouser(5, "HBase", 7000, "Bangalore",7)
  )

  val ds = spark.createDataset(objs)

  // filter all record where fee is more than 5000
  val x = ds.filter(_.fee > 5000)

  // show the explain plan of your filter
  x.explain(true)

  // df with null values
  val data = Seq(
    (1001, "Amit", "amit@hadoopexam.com", "Mumbai", 7000),
    (1002, "Rakesh", "rakesh@hadoopexam.com", null, 8000),
    (1003, "Rohit", "rohit@hadoopexam.com", "Pune", 9000),
    (1004, "Vinod", "vinod@hadoopexam.com", null, 8000),
    (1005, "Venu", "venu@hadoopexam.com", null, 6000),
    (1006, "Shyam", "shyam@hadoopexam.com", "Newyork", 8000),
    (1007, null, "john@hadoopexam.com", null, 6000)
  )

  // create df using above data
  val df3 = data.toDF("id", "name", "email", "city", "fee")

  // filter and show all the records which has city as null
  df3.filter(expr("city is null"))

  // replace all the unknown city literal with UNKNOWN
  df3.na.fill(Map("city" -> "UNKNOWN", "name" -> "anonymous")).show()

  // get the total fee across all the records
  df3.select(sum("fee")).show

  // find all the distinct values of fee collected
  df3.select(col("fee")).distinct.show

  // get the total of all distinct fees
  df3.select(sumDistinct("fee")).show

  val df4 = spark.read.format("csv").option("header", "true")
    .option("sep", "|").option("inferSchema", "true").load("/home/ayush/Desktop/extra/Databricks-Spark/file.csv")
  df4.show()

  // change the type for columns sub_start_date, sub_end_date to date instead of string where
  // new column name should be StartDate, EndDate. Existing columns should be dropped.
  df4.withColumn("StartDate", to_date(col("sub_start_date")))
    .withColumn("EndDate", col("sub_end_date").cast(DateType))
    .drop("sub_start_date", "sub_end_date").show

  // using the regex, extract only the date and ignore the time part, in string type
  val regExp = " - - "
//  df4.withColumn(col("sub_start_date").)

  // split the location column in two separate columns City and State and drop existing location column
  df4.withColumn("City", split(col("location"), "-")(0))
    .withColumn("State", split(col("location"), "-")(1))
    .drop("location").show()

  // subscription length in number of days, month, month with upto one decimal place, absolute month
  df4.withColumn("SubscriptionLengthInDays", datediff(col("sub_end_date"), col("sub_start_date")))
  .withColumn("SubscriptionLengthInMonths", datediff(col("sub_end_date"), col("sub_start_date")) / 30)
  .withColumn("SubscriptionLengthInMonthsD", round(datediff(col("sub_end_date"), col("sub_start_date")) / 30, 1))
  .withColumn("SubscriptionLengthInAbsMonth", round(datediff(col("sub_end_date"), col("sub_start_date")) / 30).cast("integer"))
  .withColumn("SubscriptionLengthInCeil", ceil(datediff(col("sub_end_date"), col("sub_start_date")) / 30).cast("integer"))
    .show()

  // extended subscription date by 90 days with date format dd-mm-yyyy

  df4.withColumn("ExtendedDate", date_format(date_add(col("sub_end_date"), 90), "dd-MM-yyyy")).show

  // read json file
  val df5 = spark.read.format("json").load("/home/ayush/Desktop/extra/Databricks-Spark/jsonFile.json")

  // define custom schema
  val schema = StructType(
    Array(
      StructField("id", LongType),
      StructField("fee", LongType),
      StructField("duration", LongType),
      StructField("name", StringType),
      StructField("venue", StringType)
    )
  )

  val df6 = spark.read.format("json").schema(schema).load("/home/ayush/Desktop/extra/Databricks-Spark/jsonFile.json")

  val rowData = spark.sparkContext.parallelize(Seq(
    Row(1001, "Amit Kumar", 10000.0, "Mumbai", 5),
    Row(1002, "John", 10000.0, "Mumbai", 5),
    Row(1003, "Venkat", 10000.0, "Delhi", 5),
    Row(1004, "Sarfraj", 10000.0, "Kolkata", 5)
  ))

  val rowSchema = StructType(
    Array(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("fee", DoubleType),
      StructField("venue", StringType),
      StructField("duration", IntegerType)
    )
  )

  val df7 = spark.createDataFrame(rowData, rowSchema)

  df7.select(col("id"), col("name")).show()
  df7.selectExpr("*", "id As COURSE_ID").show()

  val schema8 = StructType(
    Array(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("fee", DoubleType, nullable = false),
      StructField("venue", StringType, nullable = false),
      StructField("duration", IntegerType, nullable = false)
    )
  )

  val df8_1 = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
    Row(1001, "Amit Kumar", 10000.0, "Mumbai", 5),
    Row(1002, "John", 9000.0, "Mumbai", 5),
    Row(1003, "Venkat", 6000.0, "Delhi", 5),
    Row(1004, "Sarfraj", 12000.0, "Kolkata", 5)
  )), schema8)

  val df8_2 = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
    Row(1005, "Manoj" , 15000.0 , "Banglore" , 5),
    Row(1006, "Jasmin" , 16000.0 , "Mumbai" , 5),
    Row(1007, "Reegal" , 8000.0 , "Banglore" , 5),
    Row(1008, "Sayed" , 7000.0 , "Banglore" , 5)
  )), schema8)

  val unionDF = df8_1.union(df8_2)
  unionDF.show
  df8_1.filter("venue = 'Mumbai'").union(df8_2.filter("venue = 'Mumbai'")).show

  unionDF.withColumn("Website", lit("HadoopExam.com")).show

  unionDF.withColumn("FeeGreater", expr("fee > 10000")).show

  unionDF.withColumn("Website",
    when(expr("fee > 10000"), lit("Hadoop")).otherwise("Quick")).show

  unionDF.rdd.getNumPartitions
  unionDF.repartition(5, expr("venue")).coalesce(2)
  unionDF.withColumn("TotalFee", round(expr("1.18367 * fee"), 2)).show

  unionDF.selectExpr("monotonically_increasing_id() as UniqueId", "*").show

  unionDF.withColumn("City", lpad(upper(col("venue")), 3, "").as("City")).show


}
