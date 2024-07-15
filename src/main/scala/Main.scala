import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import CsvReader._
import FlightOps.FlightDatasetOps
import FrequentFlyerOps.FrequentFlyerDatasetOps
import CsvWriter.DataFrameOps

import java.sql.Date

case class Flight(passengerId: Int, flightId: Int, from: String, to: String, date: String)

case class FlightCount(month: String, count: Long)

case class FlightsTogetherBetween(passengerId1: Int, passengerId2: Int, flightsTogether: Long, from: String, to: String)

case class FlightWithParsedDate(passengerId: Int, flightId: Int, from: String, to: String, date: String, parsedDate: Date)

case class LongestRun(passengerId: Int, longestRun: Int)

case class Passenger(passengerId: Int, firstName: String, lastName: String)

case class FlightsTogether(passengerId1: Int, passengerId2: Int, flightsTogether: Long)

case class FrequentFlyer(passengerId: Int, flightCount: Long)

case class FrequentFlyerWithPassengerDetails(passengerId: Int, flightCount: Long, firstName: String, lastName: String)

object Main {

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

    DataProcessor.compute[Flight](flights,
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

    DataProcessor.compute[Flight](flights,
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

    DataProcessor.compute[Flight](flights,
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

    DataProcessor.compute[Flight](flights,
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

    DataProcessor.compute[Flight](flights,
      computeMinimumCoFLightsByPassengersBetweenDates andThen
        toCsvStructureFlightsTogetherBetween andThen
        writeFlightsTogetherBetween)

    // Stop the SparkSession
    spark.stop()
  }
}
