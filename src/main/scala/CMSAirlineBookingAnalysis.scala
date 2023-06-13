import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame
class CMSAirlineBookingAnalysis(spark: SparkSession) {
  import ColumnNames._
  import Constants._

  def runAnalysis(_bookingDataPath: String, _startDate: String, _endDate: String, _airportDataPath: String = "./data/airports/airports.dat"): Unit = {
    val bookingDataPath = _bookingDataPath
    val airportDataPath = _airportDataPath
    val startDate = _startDate
    val endDate = _endDate

    val airportDF = loadAirportData(airportDataPath)
    val bookingDF = loadBookingData(bookingDataPath)

    val filteredDF = filterData(airportDF, bookingDF)
    val finalDF = filterFinalData(filteredDF, startDate, endDate)

    val resultDF = computeResultDF(finalDF)
    val resultWithExtraInfoDF = computeResultWithExtraInfoDF(finalDF)

    displayResults(resultDF, resultWithExtraInfoDF)
  }

  private def loadAirportData(airportDataPath: String) = {
    spark.read.schema(schemas.airportSchema).csv(airportDataPath).select(IATA, Country, TimeZone)
  }

  private def loadBookingData(bookingDataPath: String) = {
    spark.read
      .option("mode", "PERMISSIVE")
      .schema(schemas.bookingSchema)
      .json(bookingDataPath)
  }

  private def filterData(airportDF: DataFrame, bookingDF: DataFrame) = {
    val airportDF_IATA_Country_TimeZone = broadcast(airportDF)

    val explodedBookingDF = bookingDF.select(col(timestamp), col(event), explode(col(NestedProductList)).as(productS)).drop(event)
    val explodedPassengerDF = bookingDF.select(col(timestamp), col(event), explode(col(NestedPassengerList)).as(passenger)).drop(event)

    val combinedDF = explodedPassengerDF.join(explodedBookingDF, Seq(timestamp))
    val combinedDF_UnNest_destination_origin_opAirline_departureDate = combinedDF
      .withColumn(destinationAirport, col(NestedDestinationAirport))
      .withColumn(originAirport, col(NestedOriginAirport))
      .withColumn(bookingStatus, col(NestedBookingStatus))
      .withColumn(operatingAirline, col(NestedOperatingAirline))
      .withColumn(departureDate, col(NestedDepartureDate))
      .withColumn(passengerUCI, col(NestedPassengerUCI))
      .withColumn(passengerType, col(NestedPassengerType))
      .withColumn(passengerAge, col(NestedPassengerAge))

    val joinedDF_ = combinedDF_UnNest_destination_origin_opAirline_departureDate
      .join(airportDF_IATA_Country_TimeZone, col(originAirport) === col(IATA))
      .withColumnRenamed(Country, OriginCountry)
      .drop(IATA)
    val joinedDF = joinedDF_.drop(passenger, productS)
    val filteredDF_ = joinedDF.filter(col(OriginCountry).isin(countryToFilterList: _*))
    val filteredDF = filteredDF_
      .join(airportDF_IATA_Country_TimeZone.drop(TimeZone), col(destinationAirport) === col(IATA))
      .withColumnRenamed(Country, DestinationCountry)
      .drop(IATA)

    filteredDF
  }

  private def filterFinalData(df: DataFrame, startDate: String, endDate: String) = {
    val sanitizeTimeZoneUDF = udf((tz: String) =>
      Option(tz).map(_.trim).filter(_.nonEmpty).getOrElse(defaultTimeZone) match {
        case "\\N" => "UTC"
        case other => other
      }
    )

    val dfWithDepatureDate = df
      .withColumn(DayOfWeek, date_format(from_utc_timestamp(col(departureDate), sanitizeTimeZoneUDF(col(TimeZone))), "EEEE"))
      .withColumn(DepartureDate, date_format(from_utc_timestamp(col(departureDate), sanitizeTimeZoneUDF(col(TimeZone))), "yyyy-MM-dd"))

    dfWithDepatureDate.filter(
      col(DepartureDate).between(startDate, endDate) &&
        col(operatingAirline) === airlineCode &&
        col(bookingStatus) === CONFIRMED
    )
  }

  private def computeResultDF(df: DataFrame) = {
    val distinctPassengersDF = df
    val dfWithSeason = distinctPassengersDF
      .withColumn(MonthS, month(col(DepartureDate)))
      .withColumn(Season, udf((month: Int) => utils.getSeason(month)).apply(col(MonthS)))
      .drop(MonthS)

    val groupedDF = dfWithSeason
      .groupBy(DestinationCountry, DayOfWeek, Season)
      .agg(
        count(passengerUCI).alias(NumberOfPassengers),
        sum(when(col(passengerType) === ADULT, 1).otherwise(0)).alias(NumberOfAdults),
        sum(when(col(passengerType) === CHILD, 1).otherwise(0)).alias(NumberOfChildren)
      )

    val windowSpec = Window.partitionBy(DestinationCountry, DayOfWeek).orderBy(col(NumberOfPassengers).desc)
    groupedDF
      .withColumn("rank", row_number().over(windowSpec))
      .filter(col("rank") === 1)
      .drop("rank")
      .orderBy(col(NumberOfPassengers).desc)
      .withColumn(Num_passengers_adultsPlusChildren, col(NumberOfAdults) + col(NumberOfChildren))
  }

  private def computeResultWithExtraInfoDF(df: DataFrame) = {
    df.groupBy(DestinationCountry)
      .agg(
        count(when(col(passengerType) === ADULT, true)).alias(NumberOfAdults),
        count(when(col(passengerType) === CHILD, true)).alias(NumberOfChildren),
        avg(passengerAge).alias(AverageAge)
      )
  }

  private def displayResults(resultDF: DataFrame, resultWithExtraInfoDF: DataFrame): Unit = {
    resultDF.show()
    resultWithExtraInfoDF.show()
  }
}
