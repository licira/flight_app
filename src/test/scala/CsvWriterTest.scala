import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}

import java.io.{File, IOException}
import java.nio.file.{Files, Path, Paths}
import java.util.Comparator
import scala.reflect.ClassTag

case class FlightCount(month: String, count: Long)

case class FrequentFlyer(passengerId: Int, flightCount: Long)

class CsvWriterSpec extends AnyFlatSpec with Matchers {

  private implicit val spark: SparkSession = SparkSession.builder()
    .appName("CsvWriterSpec")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // FlightCount helpers
  private val flightCounts = Seq(
    FlightCount("2023-01", 100),
    FlightCount("2023-02", 150),
    FlightCount("2023-03", 200)
  )
  private val flightCountsRowToTuple: Row => Product =
    r => (r.get(0), r.get(1))
  private val writtenFlightCountsRowToTuple: Row => Product =
    r => (r.get(0), r.getAs[String](1).toInt)
  private val flightCountsToTuple: FlightCount => Product =
    f => (f.month, f.count)
  // FrequentFlyer helpers
  private val frequentFlyers = Seq(
    FrequentFlyer(1, 2),
    FrequentFlyer(3, 4),
    FrequentFlyer(5, 6)
  )
  private val writtenFrequentFlyersRowToTuple: Row => Product =
    r => (r.getAs[String](0).toInt, r.getAs[String](1).toLong)
  private val frequentFlyersRowToTuple: Row => Product =
    r => (r.get(0), r.get(1))
  private val frequentFlyersToTuple: FrequentFlyer => Product =
    f => (f.passengerId, f.flightCount)
  // FrequentFlyerWithPassengerDetails
  private val frequentFlyerWithPassengerDetails = Seq(
    FrequentFlyerWithPassengerDetails(1, 2, "Napoleon", "Gaylene"),
    FrequentFlyerWithPassengerDetails(3, 4, "Katherin", "Shanell"),
    FrequentFlyerWithPassengerDetails(5, 6, "Stevie", "Steven")
  )
  private val writtenFrequentFlyerWithPassengerDetailsRowToTuple: Row => Product =
    r => (r.getAs[String](0).toInt, r.getAs[String](1).toInt, r.get(2), r.get(3))
  private val frequentFlyerWithPassengerDetailsRowToTuple: Row => Product =
    r => (r.get(0), r.get(1), r.get(2), r.get(3))
  private val frequentFlyerWithPassengerDetailsToTuple: FrequentFlyerWithPassengerDetails => Product = {
    f => (f.passengerId, f.flightCount, f.firstName, f.lastName)
  }
  // LongestRun helpers
  private val longestRuns = Seq(
    LongestRun(1, 2),
    LongestRun(3, 4),
    LongestRun(5, 6)
  )
  private val writtenLongestRunsToTupleRowToTuple: Row => Product =
    r => (r.getAs[String](0).toInt, r.getAs[String](1).toInt)
  private val longestRunsRowToTuple: Row => Product =
    r => (r.get(0), r.get(1))
  private val longestRunsToTuple: LongestRun => Product =
    f => (f.passengerId, f.longestRun)
  // FlightsTogether helper
  private val flightsTogether = Seq(
    FlightsTogether(1, 2, 3),
    FlightsTogether(3, 4, 5),
    FlightsTogether(5, 6, 7)
  )
  private val writtenFlightsTogetherToTupleRowToTuple: Row => Product =
    r => (r.getAs[String](0).toInt, r.getAs[String](1).toInt, r.getAs[String](2).toInt)
  private val flightsTogetherRowToTuple: Row => Product =
    r => (r.get(0), r.get(1), r.get(2))
  private val flightsTogetherToTuple: FlightsTogether => Product =
    f => (f.passengerId1, f.passengerId2, f.flightsTogether)
  // FlightsTogetherBetween helper
  private val flightsTogetherBetween = Seq(
    FlightsTogetherBetween(1, 2, 3, "2024-01-01", "2024-01-05"),
    FlightsTogetherBetween(3, 4, 5, "2024-01-01", "2024-01-05"),
    FlightsTogetherBetween(5, 6, 7, "2024-01-01", "2024-01-05")
  )
  private val writtenFlightsTogetherBetweenToTupleRowToTuple: Row => Product =
    r => (r.getAs[String](0).toInt, r.getAs[String](1).toInt, r.getAs[String](2).toInt, r.get(3), r.get(4))
  private val flightsTogetherBetweenRowToTuple: Row => Product =
    r => (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4))
  private val flightsTogetherBetweenToTuple: FlightsTogetherBetween => Product =
    f => (f.passengerId1, f.passengerId2, f.flightsTogether, f.from, f.to)

  "CsvWriter" should "correctly convert FlightCount to CSV structure" in {
    verifyCsvContent[FlightCount](flightCounts.toDS(),
      flightCountsRowToTuple,
      flightCountsToTuple)
  }

  "CsvWriter" should "correctly convert FrequentFlyer to CSV structure" in {
    verifyCsvContent[FrequentFlyer](frequentFlyers.toDS(),
      frequentFlyersRowToTuple,
      frequentFlyersToTuple)
  }

  "CsvWriter" should "correctly convert FrequentFlyerWithPassengerDetails to CSV structure" in {
    verifyCsvContent[FrequentFlyerWithPassengerDetails](frequentFlyerWithPassengerDetails.toDS(),
      frequentFlyerWithPassengerDetailsRowToTuple,
      frequentFlyerWithPassengerDetailsToTuple)
  }

  "CsvWriter" should "correctly convert LongestRun to CSV structure" in {
    verifyCsvContent[LongestRun](longestRuns.toDS(),
      longestRunsRowToTuple,
      longestRunsToTuple)
  }

  "CsvWriter" should "correctly convert FlightsTogether to CSV structure" in {
    verifyCsvContent[FlightsTogether](flightsTogether.toDS(),
      flightsTogetherRowToTuple,
      flightsTogetherToTuple)
  }

  "CsvWriter" should "correctly convert FlightsTogetherBetween to CSV structure" in {
    verifyCsvContent[FlightsTogetherBetween](flightsTogetherBetween.toDS(),
      flightsTogetherBetweenRowToTuple,
      flightsTogetherBetweenToTuple)
  }

  "CsvWriter" should "correctly write FlightCount to CSV file" in {
    val output = "flightCount.csv"

    val expectedColumns = Seq("Month",
      "Number of Flights")

    verifyCsvStructureAndContent[FlightCount](flightCounts.toDS(),
      output,
      expectedColumns,
      writtenFlightCountsRowToTuple,
      flightCountsToTuple)
  }

  "CsvWriter" should "correctly write FrequentFlyer to CSV file" in {
    val output = "frequentFlyer.csv"

    val expectedColumns = Seq("Passenger ID",
      "Flight Count")

    verifyCsvStructureAndContent[FrequentFlyer](frequentFlyers.toDS(),
      output,
      expectedColumns,
      writtenFrequentFlyersRowToTuple,
      frequentFlyersToTuple)
  }

  "CsvWriter" should "correctly write FrequentFlyerWithPassengerDetails to CSV file" in {
    val output = "frequentFlyerWithPassengerDetails.csv"

    val expectedColumns = Seq("Passenger ID",
      "Flight Count",
      "First Name",
      "Last Name")

    verifyCsvStructureAndContent[FrequentFlyerWithPassengerDetails](frequentFlyerWithPassengerDetails.toDS(),
      output,
      expectedColumns,
      writtenFrequentFlyerWithPassengerDetailsRowToTuple,
      frequentFlyerWithPassengerDetailsToTuple)
  }

  "CsvWriter" should "correctly write LongestRun to CSV file" in {
    val output = "longestRuns.csv"

    val expectedColumns = Seq("Passenger ID",
      "Longest Run")

    verifyCsvStructureAndContent[LongestRun](longestRuns.toDS(),
      output,
      expectedColumns,
      writtenLongestRunsToTupleRowToTuple,
      longestRunsToTuple)
  }

  "CsvWriter" should "correctly write FlightsTogether to CSV file" in {
    val output = "flightsTogether.csv"

    val expectedColumns = Seq("Passenger 1 ID",
      "Passenger 2 ID",
      "Number of flights together")

    verifyCsvStructureAndContent[FlightsTogether](flightsTogether.toDS(),
      output,
      expectedColumns,
      writtenFlightsTogetherToTupleRowToTuple,
      flightsTogetherToTuple)
  }

  "CsvWriter" should "correctly write FlightsTogetherBetween to CSV file" in {
    val output = "flightsTogetherBetween.csv"

    val expectedColumns = Seq("Passenger 1 ID",
      "Passenger 2 ID",
      "Number of flights together",
      "From",
      "To")

    verifyCsvStructureAndContent[FlightsTogetherBetween](flightsTogetherBetween.toDS(),
      output,
      expectedColumns,
      writtenFlightsTogetherBetweenToTupleRowToTuple,
      flightsTogetherBetweenToTuple)
  }

  private def readCsv(path: String,
                      spark: SparkSession): DataFrame = {
    spark.read
      .option("header", "true")
      .csv(path)
      .cache()
  }

  private def verifyCsvStructureAndContent[T: Encoder : ClassTag](ds: Dataset[T],
                                                                  path: String,
                                                                  expectedColumns: Seq[String],
                                                                  rowToTuple: Row => Product,
                                                                  toTuple: T => Product): Unit = {
    val df: DataFrame = CsvWriter.toCsvStructure[T](ds)
    CsvWriter.writeToCsv(df, path)

    val writtenData = readCsv(path, spark)

    val actualColumns = writtenData.columns
    expectedColumns should contain theSameElementsAs actualColumns

    val actualData = writtenData.collect().map(rowToTuple)
    val expectedData = ds.collect().map(toTuple)
    expectedData should contain theSameElementsAs actualData

    clean(path)
  }

  private def verifyCsvContent[T: Encoder : ClassTag](data: Dataset[T],
                                                      rowToTuple: Row => Product,
                                                      toTuple: T => Product): Unit = {
    val actualData = CsvWriter.toCsvStructure[T](data)
      .collect()
      .map(rowToTuple)

    val expectedData = data.collect()
      .map(toTuple)

    actualData should contain theSameElementsAs expectedData
  }

  private def clean(path: String): Unit = {
    // Clean up the temporary CSV file
    val dirPath: Path = Paths.get(path)
    // Delete the directory and its contents
    try {
      Files.walk(dirPath)
        .sorted(Comparator.reverseOrder())
        .forEach(Files.delete)
    } catch {
      case e: IOException => e.printStackTrace()
    }
  }
}
