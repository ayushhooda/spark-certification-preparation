package hadoopexamass1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Assessment3 extends App {

  val spark = SparkSession.builder().appName("assessment-3").master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  case class Ques1(id: Int, name: String, gender: String, salary: Int, department: String)

  val data = Seq(
    Ques1(1, "Deva", "Male", 5000, "Sales"),
    Ques1(2, "Jugnu", "Female", 6000, "HR"),
    Ques1(3, "Kavita", "Female", 7500, "IT"),
    Ques1(4, "Vikram", "Male", 6500, "Marketing"),
    Ques1(5, "Shabana", "Female", 5500, "Finance"),
    Ques1(6, "Shantilal", "Male", 8000, "Sales"),
    Ques1(7, "Vinod", "Male", 7200, "HR"),
    Ques1(8, "Vimla", "Female", 6600, "IT"),
    Ques1(9, "Jasmin", "Female", 5400, "Marketing"),
    Ques1(10, "Lovely", "Female", 6300, "Finance"),
    Ques1(11, "Mohan", "Male", 5700, "Sales"),
    Ques1(12, "Purvish", "Male", 7000, "HR"),
    Ques1(13, "Jinat", "Female", 7100, "IT"),
    Ques1(14, "Eva", "Female", 6800,"Marketing"),
    Ques1(15, "Jitendra", "Male", 5000, "Finance"),
    Ques1(15, "Rajkumar", "Male", 4500, "Finance"),
    Ques1(15, "Satish", "Male", 4500, "Finance"),
    Ques1(15, "Himmat", "Male", 3500, "Finance")
  )

  val ds1 = spark.createDataset(data)

  val windowSpec = Window.partitionBy(col("department"), col("gender"))
    .orderBy(col("salary").isin().desc_nulls_last)

  ds1.withColumn("rank", dense_rank over windowSpec).show()
  ds1.withColumn("percentRank", percent_rank() over windowSpec).show()

//  ds1.groupBy("department", "gender").pivot("salary").sum("salary").show()
  ds1.rollup("department").avg("salary").show()
  ds1.cube("department").avg("salary").show()
  ds1.randomSplit(Array(0.5, 0.5))(0).show()




}
