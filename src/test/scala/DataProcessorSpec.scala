import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Date

class DataProcessorSpec extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("FlightDatasetOpsTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val target = DataProcessor(spark)

  "DataProcessor" should "get top n most frequent flyers" in {
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

    val passengers: Dataset[Passenger] = Seq(
      Passenger(1, "Napoleon", "Gaylene"),
      Passenger(2, "Katherin", "Shanell"),
      Passenger(3, "Stevie", "Steven")
    ).toDS()

    val expectedMostFrequent = Seq(
      FrequentFlyerWithPassengerDetails(1, 4L, "Napoleon", "Gaylene"),
      FrequentFlyerWithPassengerDetails(2, 3L, "Katherin", "Shanell"),
      FrequentFlyerWithPassengerDetails(3, 2L, "Stevie", "Steven")
    ).toDS()

    val actualMostFrequent = target.mostFrequentFlyers(flights,
      passengers,
      3)

    actualMostFrequent.collect() should contain theSameElementsAs expectedMostFrequent.collect()
  }

  "DataProcessor" should "count flights by month" in {
    val flights = Seq(
      Flight(1, 1, "us", "ca", "2023-01-01"),
      Flight(1, 1, "us", "ca", "2023-01-01"),
      Flight(1, 1, "us", "ca", "2023-01-01"),
      Flight(1, 1, "us", "ca", "2023-01-01"),
      Flight(2, 2, "ca", "us", "2023-02-15"),
      Flight(2, 2, "ca", "us", "2023-02-10"),
      Flight(2, 2, "us", "ca", "2023-02-01"),
      Flight(2, 3, "us", "ca", "2023-02-20"),
      Flight(3, 3, "us", "ca", "2023-03-01"),
      Flight(3, 3, "us", "ca", "2023-03-01")).toDS()

    val expectedFlightsByMonth = Seq(
      FlightCount("1", 1L),
      FlightCount("2", 2L),
      FlightCount("3", 1L)
    ).toDS()

    val actualFlightsByMonth = target.countFlightsByMonth(flights)

    actualFlightsByMonth.collect() should contain theSameElementsAs expectedFlightsByMonth.collect()
  }

  "DataProcessor" should "compute longest streak of flight bypassing a given country" in {
    val flights = Seq(
      Flight(1, 1, "uk", "fr", "2023-01-01"),
      Flight(1, 2, "fr", "us", "2023-01-01"),
      Flight(1, 3, "us", "cn", "2023-01-01"),
      Flight(1, 4, "cn", "uk", "2023-01-01"),
      Flight(1, 5, "uk", "de", "2023-01-01"),
      Flight(1, 6, "de", "uk", "2023-01-01"),
      Flight(2, 7, "uk", "us", "2023-01-01"),
      Flight(2, 8, "us", "us", "2023-01-01"),
      Flight(2, 9, "us", "cn", "2023-01-01"),
      Flight(2, 10, "cn", "uk", "2023-01-01")).toDS()

    val expectedLongestRuns = Seq(
      LongestRun(1, 3),
      LongestRun(2, 2)
    ).toDS()

    val actualLongestRuns = target.longestRunBypassingCountry(flights, "uk")

    actualLongestRuns.collect() should contain theSameElementsAs expectedLongestRuns.collect()
  }

  "DataProcessor" should "compute flight passengers pairs that flown together a minimum amount of times" in {
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

    val actualCoflights = target.minimumCoFlightsByPassengers(flights, 2)

    actualCoflights.collect() should contain theSameElementsAs expectedCoflights.collect()
  }

  "DataProcessor" should "compute flight passengers pairs that flown together a minimum amount of times in date range" in {
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

    val actualCoflightsBetween = target.minimumCoFLightsByPassengersBetweenDates(flights, 2, from, to)

    actualCoflightsBetween.collect() should contain theSameElementsAs expectedCoflightsBetween.collect()
  }
}
