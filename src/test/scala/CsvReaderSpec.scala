import CsvReader.{flightSchema, passengerSchema, readCsv}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class CsvReaderSpec extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("CsvReaderTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  "CsvReader" should "correctly read Flight CSV file" in {
    val readFlightCsv = readCsv[Flight](flightSchema)

    // Create a temporary CSV file
    val csv = createCsvFile(
      """passengerId,flightId,from,to,date
        |1,1,us,ca,2023-01-01
        |2,2,ca,uk,2023-01-02
        |3,3,uk,de,2023-02-01""".stripMargin)

    val expectedFlights = Seq(Flight(1, 1, "us", "ca", "2023-01-01"),
      Flight(2, 2, "ca", "uk", "2023-01-02"),
      Flight(3, 3, "uk", "de", "2023-02-01")
    )
    val actualFlights = readFlightCsv(spark, csv.getAbsolutePath)
      .collect()
    actualFlights should contain theSameElementsAs expectedFlights

    csv.delete() should be(true)
  }

  "CsvReader" should "correctly read Passenger CSV file" in {
    val readPassengerCsv = readCsv[Passenger](passengerSchema)

    // Create a temporary CSV file
    val csv = createCsvFile(
      """passengerId,firstName,lastName
        |14751,Napoleon,Gaylene
        |2359,Katherin,Shanell
        |5872,Stevie,Steven""".stripMargin)

    val expectedPassengers = Seq(Passenger(14751, "Napoleon", "Gaylene"),
      Passenger(2359, "Katherin", "Shanell"),
      Passenger(5872, "Stevie", "Steven")
    )
    val actualPassengers = readPassengerCsv(spark, csv.getAbsolutePath)
      .collect()
    actualPassengers should contain theSameElementsAs expectedPassengers

    csv.delete() should be(true)
  }

  private def createCsvFile(content: String): File = {
    val tempFile = java.io.File.createTempFile("test", ".csv")
    val writer = new java.io.PrintWriter(tempFile)
    try {
      writer.write(content)
    } finally {
      writer.close()
    }
    tempFile
  }
}
