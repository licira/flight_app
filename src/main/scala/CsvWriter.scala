import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SaveMode}

import scala.reflect.ClassTag

/**
 * Object to write Datasets to CSV files.
 */
object CsvWriter {

  /**
   * Implicit class to add methods to DataFrame for writing to CSV.
   * @param df The DataFrame to be extended.
   */
  implicit class DataFrameOps(df: DataFrame) {

    /**
     * Writes the DataFrame to a CSV file.
     *
     * @param path The path to the output CSV file.
     * @param header Whether to include the header in the CSV file. Default is true.
     * @param saveMode The save mode for writing the CSV file. Default is SaveMode.Overwrite.
     * @return The original DataFrame.
     */
    def writeToCsv(path: String,
                   header: Boolean = true,
                   saveMode: SaveMode = SaveMode.Overwrite): DataFrame = {
      df.coalesce(1)
        .write
        .mode(saveMode)
        .option("header", header.toString)
        .csv(path)
      df
    }
  }

  /**
   * Writes the DataFrame to a CSV file.
   *
   * @param df The DataFrame to be written.
   * @param path The path to the output CSV file.
   * @param header Whether to include the header in the CSV file. Default is true.
   * @param saveMode The save mode for writing the CSV file. Default is SaveMode.Overwrite.
   * @return The original DataFrame.
   */
  def writeToCsv(df: DataFrame,
                 path: String,
                 header: Boolean = true,
                 saveMode: SaveMode = SaveMode.Overwrite): DataFrame = {
    df.coalesce(1)
      .write
      .mode(saveMode)
      .option("header", header.toString)
      .csv(path)
    df
  }

  /**
   * Converts a Dataset to a DataFrame with specific column names for CSV output.
   *
   * @tparam T The type of the Dataset.
   * @param ds The Dataset to be converted.
   * @return A DataFrame with columns renamed for CSV output.
   * @throws IllegalArgumentException if the type T is not supported.
   */
  def toCsvStructure[T: Encoder : ClassTag](ds: Dataset[T]): DataFrame = {
    implicitly[ClassTag[T]] match {
      case ct if ct.runtimeClass == classOf[FlightCount] =>
        ds.withColumnRenamed("month", "Month")
          .withColumnRenamed("count", "Number of Flights")
      case ct if ct.runtimeClass == classOf[FrequentFlyer] =>
        ds.withColumnRenamed("passengerId", "Passenger ID")
          .withColumnRenamed("flightCount", "Flight Count")
          .withColumnRenamed("firstName", "First Name")
          .withColumnRenamed("lastName", "Last Name")
      case ct if ct.runtimeClass == classOf[FrequentFlyerWithPassengerDetails] =>
        ds.withColumnRenamed("passengerId", "Passenger ID")
          .withColumnRenamed("flightCount", "Flight Count")
          .withColumnRenamed("firstName", "First Name")
          .withColumnRenamed("lastName", "Last Name")
      case ct if ct.runtimeClass == classOf[LongestRun] =>
        ds.withColumnRenamed("passengerId", "Passenger ID")
          .withColumnRenamed("longestRun", "Longest Run")
      case ct if ct.runtimeClass == classOf[FlightsTogether] =>
        ds.withColumnRenamed("passengerId1", "Passenger 1 ID")
          .withColumnRenamed("passengerId2", "Passenger 2 ID")
          .withColumnRenamed("flightsTogether", "Number of flights together")
      case ct if ct.runtimeClass == classOf[FlightsTogetherBetween] =>
        ds.select(col("passengerId1").as("Passenger 1 ID"),
          col("passengerId2").as("Passenger 2 ID"),
          col("flightsTogether").as("Number of flights together"),
          col("from").as("From"),
          col("to").as("To"))
    }
  }
}