package utilities

import org.apache.spark.sql.SparkSession

object SparkUtilites {

  def getSparkSession(appName: String, master: String = "local"): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .master(master)
      .getOrCreate()
  }

}
