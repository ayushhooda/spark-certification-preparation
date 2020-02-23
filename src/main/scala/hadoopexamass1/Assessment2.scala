package hadoopexamass1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

object Assessment2 extends App {

  val spark = SparkSession.builder.appName("assessment-2").master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  // set checkpointing directory
  spark.sparkContext
    .setCheckpointDir("/home/ayush/Desktop/extra/Databricks-Spark/src/main/scala/hadoopexamass1/savedData")

  import spark.implicits._

  val df1 = spark.read.format("json").load("/home/ayush/Desktop/extra/Databricks-Spark/src/main/scala/hadoopexamass1/json1.json",
  "/home/ayush/Desktop/extra/Databricks-Spark/src/main/scala/hadoopexamass1/json2.json")

  df1.printSchema()

  df1.inputFiles(0)
  df1.inputFiles(1)

  df1.isLocal
  df1.columns
  df1.dtypes
  df1.createTempView("local1")
  df1.schema.json
  df1.schema.sql
  df1.schema.prettyJson
  df1.schema.simpleString
  df1.schema.printTreeString
  df1.schema.catalogString

  val df5 = spark.read.format("json").load("/home/ayush/Desktop/extra/Databricks-Spark/src/main/scala/hadoopexamass1/json1.json")
  df5.createOrReplaceTempView("myTable")
  print(spark.catalog.tableExists("myTable"))

  spark.read.table("myTable")
  spark.sql("select count(*) from myTable").show
  spark.sql("select avg(fee) from myTable").show
  spark.sql("select * from myTable where fee > 5000").show

  val df6 = spark.read.format("csv").option("inferSchema", "true").option("header", "true")
    .load("/home/ayush/Desktop/extra/Databricks-Spark/src/main/scala/hadoopexamass1/data_HadooExam_Training.csv")

  case class HECourse(id: Int, name: String, fee: Double, venue: String, date: String, duration: Int)

//  implicit val enc: Encoder[HECourse] = Encoders.product[HECourse]

  val ds6 = df6.as[HECourse]

  ds6.cache()
  ds6.unpersist()

  df6.select($"ID", $"name").explain(true)

  // checkpoint the dataset
  ds6.show(5)
  ds6.checkpoint()

//  val newDF = spark.sparkContext.getCheckpointDir.map(spark.read.format("parqu").load(_)).get
//  newDF.show()

  ds6.select(sum(col("duration"))).show()



  val struct = StructType.fromDDL("name STRING, rank INT")

//  udfExampleDF.hint()

  case class Ques8(id: Int, name: String, fee: Double, venue: String, duration: Int)


  val ds8_1 = spark.createDataset(Seq(
    Ques8(1, "Hadoop", 6000, "Mumbai", 5),
    Ques8(2, "Spark", 5000, "Pune", 4),
    Ques8(3, "Python", 4000, "Hyderabad", 3),
    Ques8(4, "Scala", 4000, "Kolkata", 3),
    Ques8(5, "HBase", 7000, "Banglore", 7),
    Ques8(4, "Scala", 4000, "Kolkata", 3),
    Ques8(5, "HBase", 7000, "Banglore", 7),
    Ques8(11, "Scala", 4000, "Kolkata", 3),
    Ques8(12, "HBase", 7000, "Banglore", 7)
  ))

  val ds8_2 = spark.createDataset(Seq(
    Ques8(1, "Hadoop", 6000, "Mumbai", 5),
    Ques8(12, "Spark", 5000,"Pune", 4),
    Ques8(13, "Python", 4000, "Hyderabad", 3)
  ))

  // apply inner, left outer, right outer joins
  ds8_1.joinWith(ds8_2, ds8_1("id") === ds8_2("id"), "inner").printSchema()


}
