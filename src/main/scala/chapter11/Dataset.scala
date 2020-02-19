package chapter11

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

object Dataset extends App {

  val spark = SparkSession.builder().appName("Dataset").master("local[*]").getOrCreate()

  // To efficiently support domain-specific objects, a
  // special concept called an “Encoder” is required. The encoder maps the domain-specific type T to
  // Spark’s internal type system.

  // Creating Datasets
  case class Flight(DEST_COUNTRY_NAME: String,
                    ORIGIN_COUNTRY_NAME: String,
                    count: BigInt)

  val flightsDF = spark.read
    .parquet("/data/flight-data/parquet/2010-summary.parquet/")

  implicit val flightEncoder: Encoder[Flight] = Encoders.product[Flight]

  val flights = flightsDF.as[Flight]
  flights.show(2)

}
