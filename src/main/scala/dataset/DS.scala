package dataset

import org.apache.spark.sql.Dataset
import utilities.SparkUtilites

object DS extends App {

  val spark = SparkUtilites.getSparkSession("ds")

  import spark.implicits._

  val wordsDataset: Dataset[String] = spark.sparkContext.parallelize(Seq("Spark I am your father", "May the spark be with you", "Spark I am your father")).toDS()
  val groupedDataset = wordsDataset.flatMap(_.toLowerCase.split(" "))
    .filter(_ != "")
    .groupBy("value")
  val countsDataset = groupedDataset.count()
  countsDataset.show()


}
