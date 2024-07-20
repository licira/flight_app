import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, Dataset, SparkSession}

import scala.collection.immutable.ListMap

/**
 * Class to write datasets to CSV files with specified column mappings.
 *
 * @param spark The SparkSession used for writing datasets.
 */
class CsvWriter(spark: SparkSession) {

  import spark.implicits._

  /**
   * Writes a dataset to a CSV file with specified column mappings.
   *
   * @param ds The dataset to write.
   * @param columnMappings A map of original column names to new column names.
   * @param outputPath The output path where the CSV file will be written.
   * @tparam T The type of the dataset.
   */
  def writeDataset[T](ds: Dataset[T],
                      columnMappings: Map[String, String],
                      outputPath: String): Unit = {
    val selectedColumns: Seq[Column] = columnMappings.map {
      case (originalName, newName) => col(originalName).as(newName).as[String]
    }.toSeq

    ds.select(selectedColumns: _*)
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputPath)
  }
}

/**
 * Companion object for CsvWriter.
 */
object CsvWriter {

  /**
   * Column mappings for FlightCount dataset.
   */
  val flightCountColumnMappings = ListMap(
    "month" -> "Month",
    "count" -> "Number of Flights"
  )

  /**
   * Column mappings for FrequentFlyerWithPassengerDetails dataset.
   */
  val frequentFlyerWithPassengerDetailsColumnMappings = ListMap(
    "passengerId" -> "Passenger ID",
    "flightCount" -> "Flight Count",
    "firstName" -> "First Name",
    "lastName" -> "Last Name"
  )

  /**
   * Column mappings for FrequentFlyer dataset.
   */
  val frequentFlyerColumnMappings = ListMap(
    "passengerId" -> "Passenger ID",
    "longestRun" -> "Longest Run"
  )

  /**
   * Column mappings for FlightsTogether dataset.
   */
  val minimumCoFlightsColumnMappings = ListMap(
    "passengerId1" -> "Passenger 1 ID",
    "passengerId2" -> "Passenger 2 ID",
    "flightsTogether" -> "Number of flights together"
  )

  /**
   * Column mappings for FlightsTogetherBetween dataset.
   */
  val minimumCoFlightsBetweenDatesColumnMappings = ListMap(
    "passengerId1" -> "Passenger 1 ID",
    "passengerId2" -> "Passenger 2 ID",
    "flightsTogether" -> "Number of flights together",
    "from" -> "From",
    "to" -> "To"
  )

  /**
   * Creates a new instance of CsvWriter.
   *
   * @param spark The SparkSession used for writing datasets.
   * @return A new instance of CsvWriter.
   */
  def apply(spark: SparkSession): CsvWriter = new CsvWriter(spark)
}