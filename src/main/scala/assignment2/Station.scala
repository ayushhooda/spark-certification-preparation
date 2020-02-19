package assignment2

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoder, Encoders}

case class Station(
                    stationId: Int,
                    name: String,
                    lat: Double,
                    lon: Double,
                    dockCount: Int,
                    landmark: String,
                    installation: String
                  )

object Station {
  implicit val stationEncoder: Encoder[Station] = Encoders.product[Station]
  implicit val stationSchema: StructType = Encoders.product[Station].schema
}
