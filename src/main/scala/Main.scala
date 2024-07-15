import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import CsvReader._
import FlightOps.FlightDatasetOps
import FrequentFlyerOps.FrequentFlyerDatasetOps
import CsvWriter.DataFrameOps

import java.sql.Date

/**
 * Case class representing a flight.
 * @param passengerId The ID of the passenger.
 * @param flightId The ID of the flight.
 * @param from The departure location.
 * @param to The arrival location.
 * @param date The date of the flight.
 */
case class Flight(passengerId: Int, flightId: Int, from: String, to: String, date: String)

/**
 * Case class representing the count of flights per month.
 * @param month The month of the flights.
 * @param count The count of flights in the month.
 */
case class FlightCount(month: String, count: Long)

/**
 * Case class representing the number of flights two passengers have taken together within a date range.
 * @param passengerId1 The ID of the first passenger.
 * @param passengerId2 The ID of the second passenger.
 * @param flightsTogether The number of flights together.
 * @param from The start date of the date range.
 * @param to The end date of the date range.
 */
case class FlightsTogetherBetween(passengerId1: Int, passengerId2: Int, flightsTogether: Long, from: String, to: String)

/**
 * Case class representing a flight with a parsed date.
 * @param passengerId The ID of the passenger.
 * @param flightId The ID of the flight.
 * @param from The departure location.
 * @param to The arrival location.
 * @param date The date of the flight.
 * @param parsedDate The parsed date of the flight.
 */
case class FlightWithParsedDate(passengerId: Int, flightId: Int, from: String, to: String, date: String, parsedDate: Date)

/**
 * Case class representing the longest run of flights bypassing a specified country.
 * @param passengerId The ID of the passenger.
 * @param longestRun The longest run of flights bypassing the specified country.
 */
case class LongestRun(passengerId: Int, longestRun: Int)

/**
 * Case class representing a passenger.
 * @param passengerId The ID of the passenger.
 * @param firstName The first name of the passenger.
 * @param lastName The last name of the passenger.
 */
case class Passenger(passengerId: Int, firstName: String, lastName: String)

/**
 * Case class representing the number of flights two passengers have taken together.
 * @param passengerId1 The ID of the first passenger.
 * @param passengerId2 The ID of the second passenger.
 * @param flightsTogether The number of flights together.
 */
case class FlightsTogether(passengerId1: Int, passengerId2: Int, flightsTogether: Long)

/**
 * Case class representing a frequent flyer.
 * @param passengerId The ID of the passenger.
 * @param flightCount The number of flights taken by the passenger.
 */
case class FrequentFlyer(passengerId: Int, flightCount: Long)

/**
 * Case class representing a frequent flyer with passenger details.
 * @param passengerId The ID of the passenger.
 * @param flightCount The number of flights taken by the passenger.
 * @param firstName The first name of the passenger.
 * @param lastName The last name of the passenger.
 */
case class FrequentFlyerWithPassengerDetails(passengerId: Int, flightCount: Long, firstName: String, lastName: String)

/**
 * Main object for running the flight analysis application.
 */
object Main {

  /**
   * The main method for running the flight analysis application.
   *
   * @param args Command line arguments for the application.
   */
  def main(args: Array[String]): Unit = {

    // Retrieve the arguments
    val flightsDataPath = args(0)
    val passengersDataPath = args(1)
    val outputPath = if (args.length > 2) args(2) else "output/"

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Flight Analysis Application")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Initialize read functions
    val readFlightCsv = readCsv[Flight]
    val readPassengerCsv = readCsv[Passenger]

    // Read datasets
    val flights = readFlightCsv(spark, flightsDataPath)
    val passengers = readPassengerCsv(spark, passengersDataPath)

    // Q1: Find the total number of flights for each month.
    val countFlightsByMonth: Dataset[Flight] => Dataset[FlightCount] =
      _.countFlightsByMonth()
    val mapToFlightCountCsvStructure: Dataset[FlightCount] => DataFrame =
      CsvWriter.toCsvStructure[FlightCount]
    val writeFlightCount: DataFrame => DataFrame = _.writeToCsv(s"${outputPath}/q1_flightsPerMonth")

    compute[Flight](flights,
      countFlightsByMonth andThen
        mapToFlightCountCsvStructure andThen
        writeFlightCount)

    // Q2: Find the names of the 100 most frequent flyers.
    val computeMostFrequentFlyers: Dataset[Flight] => Dataset[FrequentFlyer] =
      _.computeMostFrequentFlyers(100)
    val joinWithPassengers: Dataset[FrequentFlyer] => Dataset[FrequentFlyerWithPassengerDetails] =
      _.joinWithPassengers(passengers)
    val mapToFrequentFlyerWithPassengerDetailsCsvStructure: Dataset[FrequentFlyerWithPassengerDetails] => DataFrame
    = CsvWriter.toCsvStructure[FrequentFlyerWithPassengerDetails]
    val writeFrequentFlyerWithPassengerDetails: DataFrame => DataFrame
    = _.writeToCsv(s"${outputPath}/q2_mostFrequentFlyers")

    compute[Flight](flights,
      computeMostFrequentFlyers andThen
        joinWithPassengers andThen
        mapToFrequentFlyerWithPassengerDetailsCsvStructure andThen
        writeFrequentFlyerWithPassengerDetails)

    // Q3: Find the greatest number of countries a passenger has been in without being in the UK.
    val computeLongestRunBypassingCountry: Dataset[Flight] => Dataset[LongestRun] =
      _.computeLongestRunBypassingCountry("uk")
    val mapToLongestRunCsvStructure: Dataset[LongestRun] => DataFrame =
      CsvWriter.toCsvStructure[LongestRun]
    val writeLongestRun: DataFrame => DataFrame =
      _.writeToCsv(s"${outputPath}/q3_longestRunBypassingCountry")

    compute[Flight](flights,
      computeLongestRunBypassingCountry andThen
        mapToLongestRunCsvStructure andThen
        writeLongestRun)

    // Q4: Find the passengers who have been on more than 3 flights together.
    val computeMinimumCoFlightsByPassengers: Dataset[Flight] => Dataset[FlightsTogether] =
      _.computesMinimumCoFlightsByPassengers(3)
    val mapToFlightsTogetherCsvStructure: Dataset[FlightsTogether] => DataFrame =
      CsvWriter.toCsvStructure[FlightsTogether]
    val writeFlightsTogether: DataFrame => DataFrame =
      _.writeToCsv(s"${outputPath}/q4_flightsTogether")

    compute[Flight](flights,
      computeMinimumCoFlightsByPassengers andThen
        mapToFlightsTogetherCsvStructure andThen
        writeFlightsTogether)

    // Q extra: Find the passengers who have been on more than N flights together within the range (from,to).
    val from = Date.valueOf("2017-01-01")
    val to = Date.valueOf("2017-12-31")
    val computeMinimumCoFLightsByPassengersBetweenDates: Dataset[Flight] => Dataset[FlightsTogetherBetween] =
      _.computeMinimumCoFLightsByPassengersBetweenDates(5, from, to)
    val toCsvStructureFlightsTogetherBetween: Dataset[FlightsTogetherBetween] => DataFrame =
      CsvWriter.toCsvStructure[FlightsTogetherBetween]
    val writeFlightsTogetherBetween: DataFrame => DataFrame =
      _.writeToCsv(s"${outputPath}/qextra_flightsTogetherBetween")

    compute[Flight](flights,
      computeMinimumCoFLightsByPassengersBetweenDates andThen
        toCsvStructureFlightsTogetherBetween andThen
        writeFlightsTogetherBetween)

    // Stop the SparkSession
    spark.stop()
  }

  /**
   * Applies a series of transformations to a Dataset and returns a DataFrame.
   *
   * @param ds The input Dataset.
   * @param transformations The transformations to be applied to the Dataset.
   * @tparam T The type of the input Dataset.
   * @return The transformed DataFrame.
   */
  private def compute[T](ds: Dataset[T],
    transformations: Dataset[T] => DataFrame = (ds: Dataset[T]) => ds.toDF()): DataFrame = {
    transformations(ds)
  }
}
