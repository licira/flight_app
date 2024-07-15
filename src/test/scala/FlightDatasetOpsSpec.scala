import FlightOps.FlightDatasetOps
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Date

class FlightDatasetOpsSpec extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("FlightDatasetOpsTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  "FlightDatasetOps" should "top n most frequent flyers" in {
    val flights = Seq(
      Flight(1, 1, "us", "ca", "2023-01-01"),
      Flight(1, 1, "us", "ca", "2023-02-01"),
      Flight(1, 1, "us", "ca", "2023-03-01"),
      Flight(1, 1, "us", "ca", "2023-03-01"),
      Flight(2, 2, "ca", "us", "2023-01-15"),
      Flight(2, 2, "ca", "us", "2023-02-10"),
      Flight(2, 2, "us", "ca", "2023-03-01"),
      Flight(3, 3, "us", "ca", "2023-03-01"),
      Flight(3, 3, "us", "ca", "2023-03-01"),
      Flight(4, 4, "us", "ca", "2023-03-01"),
      Flight(5, 5, "us", "ca", "2023-03-01")).toDS()

    val expectedMostFrequent = Seq(
      FrequentFlyer(1, 4L),
      FrequentFlyer(2, 3L),
      FrequentFlyer(3, 2L)
    ).toDS()

    val actualMostFrequent = flights.computeMostFrequentFlyers(3)

    actualMostFrequent.collect() should contain theSameElementsAs expectedMostFrequent.collect()
  }

  "FlightDatasetOps" should "count flights by month" in {
    val flights = Seq(
      Flight(1, 1, "us", "ca", "2023-01-01"),
      Flight(1, 1, "us", "ca", "2023-01-01"),
      Flight(1, 1, "us", "ca", "2023-01-01"),
      Flight(1, 1, "us", "ca", "2023-01-01"),
      Flight(2, 2, "ca", "us", "2023-02-15"),
      Flight(2, 2, "ca", "us", "2023-02-10"),
      Flight(2, 2, "us", "ca", "2023-02-01"),
      Flight(3, 3, "us", "ca", "2023-03-01"),
      Flight(3, 3, "us", "ca", "2023-03-01")).toDS()

    val expectedFlightsByMonth = Seq(
      FlightCount("1", 4L),
      FlightCount("2", 3L),
      FlightCount("3", 2L)
    ).toDS()

    val actualFlightsByMonth = flights.countFlightsByMonth()

    actualFlightsByMonth.collect() should contain theSameElementsAs expectedFlightsByMonth.collect()
  }

  "FlightDatasetOps" should "compute longest streak of flight bypassing a given country" in {
    val flights = Seq(
      Flight(1, 1, "us", "uk", "2023-01-01"),
      Flight(1, 2, "uk", "ca", "2023-01-01"),
      Flight(1, 3, "ca", "us", "2023-01-01"),
      Flight(1, 4, "us", "ca", "2023-01-01"),
      Flight(1, 5, "us", "de", "2023-01-01"),
      Flight(1, 6, "de", "fr", "2023-01-01"),
      Flight(1, 7, "fr", "uk", "2023-01-01"),
      Flight(2, 8, "ca", "uk", "2023-02-15"),
      Flight(2, 9, "uk", "de", "2023-02-10"),
      Flight(2, 10, "de", "fr", "2023-02-01"),
      Flight(3, 11, "fr", "uk", "2023-03-01"),
      Flight(3, 12, "uk", "us", "2023-03-01")).toDS()

    val expectedLongestRuns = Seq(
      LongestRun(1, 4),
      LongestRun(2, 1)
    ).toDS()

    val actualLongestRuns = flights.computeLongestRunBypassingCountry("uk")

    actualLongestRuns.collect() should contain theSameElementsAs expectedLongestRuns.collect()
  }

  "FlightDatasetOps" should "compute flight passengers pairs that flown together a minimum amount of times" in {
    val flights = Seq(
      Flight(1, 1, "us", "uk", "2023-01-01"),
      Flight(2, 1, "us", "uk", "2023-01-01"),
      Flight(3, 1, "us", "uk", "2023-01-01"),
      Flight(4, 1, "us", "uk", "2023-01-01"),
      Flight(1, 2, "uk", "us", "2023-01-02"),
      Flight(2, 2, "uk", "us", "2023-01-02"),
      Flight(1, 3, "us", "ca", "2023-01-03"),
      Flight(2, 3, "us", "ca", "2023-01-03")
    ).toDS()

    val expectedCoflights = Seq(
      FlightsTogether(1, 2, 3)
    ).toDS()

    val actualCoflights = flights.computesMinimumCoFlightsByPassengers(2)

    actualCoflights.collect() should contain theSameElementsAs expectedCoflights.collect()
  }

  "FlightDatasetOps" should "compute flight passengers pairs that flown together a minimum amount of times in date range" in {
    val flights = Seq(
      Flight(1, 1, "us", "uk", "2023-01-01"),
      Flight(2, 1, "us", "uk", "2023-01-01"),
      Flight(1, 2, "uk", "us", "2023-01-02"),
      Flight(2, 2, "uk", "us", "2023-01-02"),
      Flight(1, 3, "us", "ca", "2023-01-03"),
      Flight(2, 3, "us", "ca", "2023-01-03"),

      Flight(3, 4, "us", "uk", "2023-01-30"),
      Flight(4, 4, "us", "uk", "2023-01-30"),
      Flight(3, 5, "uk", "us", "2023-01-31"),
      Flight(4, 5, "uk", "us", "2023-01-31"),
      Flight(3, 6, "us", "ca", "2023-02-01"),
      Flight(4, 6, "us", "ca", "2023-02-01")
    ).toDS()

    val expectedCoflightsBetween = Seq(
      FlightsTogetherBetween(1, 2, 3, "2023-01-01", "2023-01-03")
    ).toDS()

    val from = Date.valueOf("2023-01-01")
    val to = Date.valueOf("2023-01-31")

    val actualCoflightsBetween = flights.computeMinimumCoFLightsByPassengersBetweenDates(2, from, to)

    actualCoflightsBetween.collect() should contain theSameElementsAs expectedCoflightsBetween.collect()
  }
}
