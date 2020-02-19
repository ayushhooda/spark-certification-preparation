package assignment2

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoder, Encoders}

case class Trip(
               tripId: Int,
               duration: Int,
               startDate: String,
               startStation: String,
               startTerminal: Int,
               endDate: String,
               endStation: String,
               endTerminal: Int,
               bike: Int,
               subscriberType: String,
               zipCode: String
               )

object Trip {
  implicit val tripEncoder: Encoder[Trip] = Encoders.product[Trip]
  implicit val tripSchema: StructType = Encoders.product[Trip].schema
}
