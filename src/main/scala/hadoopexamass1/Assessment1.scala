package hadoopexamass1

import org.apache.spark.sql.functions.{ expr, sum, col, sumDistinct}
import org.apache.spark.sql.{Encoders, SparkSession}

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


}
