import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, StringType, StructField, StructType}

object schemas {
  """Airport Schema """

  val airportSchema: StructType = StructType(Array(
    StructField("Airport ID", IntegerType, nullable = true),
    StructField("Name", StringType, nullable = true),
    StructField("City", StringType, nullable = true),
    StructField("Country", StringType, nullable = true),
    StructField("IATA", StringType, nullable = true),
    StructField("ICAO", StringType, nullable = true),
    StructField("Latitude", DoubleType, nullable = true),
    StructField("Longitude", DoubleType, nullable = true),
    StructField("Altitude", IntegerType, nullable = true),
    StructField("Timezone", DoubleType, nullable = true),
    StructField("DST", StringType, nullable = true),
    StructField("Tz Database time zone", StringType, nullable = true),
    StructField("Type", StringType, nullable = true),
    StructField("Source", StringType, nullable = true)
  ))

  """Booking Schema """

  val flightSchema: StructType = StructType(Array(
    StructField("operatingAirline", StringType, nullable = true),
    StructField("originAirport", StringType, nullable = true),
    StructField("destinationAirport", StringType, nullable = true),
    StructField("departureDate", StringType, nullable = true),
    StructField("arrivalDate", StringType, nullable = true)
  ))

  val productSchema: StructType = StructType(Array(
    StructField("bookingStatus", StringType, nullable = true),
    StructField("flight", flightSchema, nullable = true)
  ))

  val passengerSchema: StructType = StructType(Array(
    StructField("uci", StringType, nullable = true),
    StructField("age", IntegerType, nullable = true),
    StructField("passengerType", StringType, nullable = true)
  ))

  val travelRecordSchema: StructType = StructType(Array(
    StructField("passengersList", ArrayType(passengerSchema), nullable = true),
    StructField("productsList", ArrayType(productSchema), nullable = true)
  ))

  val dataElementSchema: StructType = StructType(Array(
    StructField("travelrecord", travelRecordSchema, nullable = true)
  ))

  val eventSchema: StructType = StructType(Array(
    StructField("DataElement", dataElementSchema, nullable = true)
  ))

  val bookingSchema: StructType = StructType(Array(
    StructField("timestamp", StringType, nullable = true),
    StructField("event", eventSchema, nullable = true)
  ))

  """ Schemas end """

}
