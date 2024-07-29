import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.IOException
import java.nio.file.{DirectoryStream, Files, Path, Paths}
import scala.io.Source
import scala.collection.immutable.ListMap

class CsvWriterSpec extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("FlightDatasetOpsTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val target = CsvWriter.apply(spark)

  "writeFlightCounts" should "preserve column order" in {
    import spark.implicits._

    val ds: Dataset[FlightCount] = Seq(FlightCount("2021-01", 100)).toDS()
    val outputPath = "output/flightCounts/"
    val columnMappings = ListMap(
      "month" -> "Month",
      "count" -> "Number of Flights"
    )

    target.writeDataset(ds, columnMappings, outputPath)

    val header = readCsvHeader(outputPath)
    assert(header == "Month,Number of Flights")

    deleteDirectory(outputPath)
  }

  "writeMostFrequentFlyers" should "preserve column order" in {
    val ds: Dataset[FrequentFlyerWithPassengerDetails] = Seq(
      FrequentFlyerWithPassengerDetails(1, 10L, "John", "Doe")
    ).toDS()
    val outputPath = "output/frequentFlyers/"
    val columnMappings = ListMap(
      "passengerId" -> "Passenger ID",
      "flightCount" -> "Flight Count",
      "firstName" -> "First Name",
      "lastName" -> "Last Name"
    )

    target.writeDataset(ds, columnMappings, outputPath)

    val header = readCsvHeader(outputPath)
    assert(header == "Passenger ID,Flight Count,First Name,Last Name")

    deleteDirectory(outputPath)
  }

  "writeLongestRuns" should "preserve column order" in {
    val ds: Dataset[LongestRun] = Seq(LongestRun(1, 5)).toDS()
    val outputPath = "output/longestRuns/"
    val columnMappings = ListMap(
      "passengerId" -> "Passenger ID",
      "longestRun" -> "Longest Run"
    )

    target.writeDataset(ds, columnMappings, outputPath)

    val header = readCsvHeader(outputPath)
    assert(header == "Passenger ID,Longest Run")

    deleteDirectory(outputPath)
  }

  "writeMinimumCoFlights" should "preserve column order" in {
    val ds: Dataset[FlightsTogether] = Seq(
      FlightsTogether(1, 2, 5)
    ).toDS()
    val outputPath = "output/minimumCoFlights/"
    val columnMappings = ListMap(
      "passengerId1" -> "Passenger 1 ID",
      "passengerId2" -> "Passenger 2 ID",
      "flightsTogether" -> "Number of flights together"
    )

    target.writeDataset(ds, columnMappings, outputPath)

    val header = readCsvHeader(outputPath)
    assert(header == "Passenger 1 ID,Passenger 2 ID,Number of flights together")

    deleteDirectory(outputPath)
  }

  "writeMinimumCoFlightsBetweenDates" should "preserve column order" in {
    val ds: Dataset[FlightsTogetherBetween] = Seq(
      FlightsTogetherBetween(1, 2, 5, "2017-01-01", "2017-01-03")
    ).toDS()
    val outputPath = "output/minimumCoFlights/"
    val columnMappings = ListMap(
      "passengerId1" -> "Passenger 1 ID",
      "passengerId2" -> "Passenger 2 ID",
      "flightsTogether" -> "Number of flights together",
      "from" -> "From",
      "to" -> "To"
    )

    target.writeDataset(ds, columnMappings, outputPath)

    val header = readCsvHeader(outputPath)
    assert(header == "Passenger 1 ID,Passenger 2 ID,Number of flights together,From,To")

    deleteDirectory(outputPath)
  }

  private def readCsvHeader(outputPath: String): String = {
    val files = Files.list(Paths.get(outputPath)).toArray
    val csvFile = files.find(_.toString.endsWith(".csv")).get.toString
    val source = Source.fromFile(csvFile)
    try {
      source.getLines().next()
    } finally {
      source.close()
    }
  }

  private def deleteDirectory(path: String): Boolean = {
    try {
      val dirPath: Path = Paths.get(path)
      if (Files.exists(dirPath) && Files.isDirectory(dirPath)) {
        deleteRecursively(dirPath)
      } else {
        false
      }
    } catch {
      case e: IOException =>
        e.printStackTrace()
        false
    }
  }

  private def deleteRecursively(path: Path): Boolean = {
    if (Files.isDirectory(path)) {
      val dirStream: DirectoryStream[Path] = Files.newDirectoryStream(path)
      try {
        dirStream.forEach(deleteRecursively)
      } finally {
        dirStream.close()
      }
    }
    Files.deleteIfExists(path)
  }
}