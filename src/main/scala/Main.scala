import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Main {
  def main(args: Array[String]): Unit = {
    try {
      val spark = SparkSession.builder
        .appName("CMSAirlineBookingAnalysis")
        .config("spark.sql.session.timeZone", "UTC") // utc mentioned in datadict
        .config("spark.master", "local[*]") // Set the default master URL for local mode
        .getOrCreate()

      val bookingDataPath = if (args.length > 0) args(0) else "./data/bookings/booking.json" // Path to the bookings data directory can also be hdfs automatically sets yarn
      val startDate = if (args.length > 1) args(1) else "2010-01-01" // Start date in yyyy-MM-dd format
      val endDate = if (args.length > 2) args(2) else "2023-01-01" // End date in yyyy-MM-dd format
      val airportDataPath = if (args.length > 3) args(3) else "./data/airports/airports.dat"

      val masterURL = getMasterURL(bookingDataPath)
      spark.conf.set("spark.master", masterURL)

      val analysis = new CMSAirlineBookingAnalysis(spark)
      analysis.runAnalysis(bookingDataPath, startDate, endDate, airportDataPath)

      spark.stop()
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
  }

  def getMasterURL(path: String): String = {
    if (path.startsWith("hdfs://")) {
      "yarn" // Set the appropriate master URL for HDFS
    } else {
      "local[*]" // Set the default master URL for local mode
    }
  }
}
