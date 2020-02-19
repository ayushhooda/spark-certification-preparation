package chapter3

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.date_format
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}

object MachineLearning extends App {

  val spark = SparkSession
    .builder
    .appName("Machine-Learning")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")


  // Reading data as batch
  val staticDataFrame = spark.read.format("csv")
    .option("header", value = true)
    .option("inferSchema", value = true)
    .load("/home/ayush/Desktop/extra/Databricks-Spark/src/main/scala/chapter3/*.csv")

  staticDataFrame.printSchema()

  /* Machine learning algorithms in MLlib require that data is represented as numerical values.
     Our current data is represented by a variety of different types, including timestamps, integers,
     and strings. Therefore we need to transform this data into some numerical representation. In
     this instance, we’ll use several DataFrame transformations to manipulate our date data: */

  val preppedDF = staticDataFrame.na.fill(0)
    .withColumn("day_of_week", date_format($"InvoiceDate", "EEEE"))
    .coalesce(5)

  val trainDF = preppedDF.where("InvoiceDate < '2011-12-09'")

  val testDF = preppedDF.where("InvoiceDate >= '2011-12-09'")

  print("\nTraining Set: " + trainDF.count)
  print("\n Test Set: " + testDF.count)

  //Spark’s MLlib also provides a number of transformations with which we can automate some of our
  //general transformations. One such transformer is a StringIndexer
  val indexer = new StringIndexer()
    .setInputCol("day_of_week")
    .setOutputCol("day_of_week_index")

  val encoder = new OneHotEncoder()
      .setInputCol("day_of_week_index")
      .setOutputCol("day_of_week_encoded")

  val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("UnitPrice", "Quantity", "day_of_week_encoded"))
      .setOutputCol("features")

  val transformationPipeline = new Pipeline()
      .setStages(Array(indexer, encoder, vectorAssembler))

  val fittedPipeline = transformationPipeline.fit(trainDF)

  val transformedTraining = fittedPipeline.transform(trainDF)

  transformedTraining.cache()

  val kmeans = new KMeans()
      .setK(20)
      .setSeed(1L)

  val kmModel = kmeans.fit(transformedTraining)

  print(kmModel.computeCost(testDF))


}
