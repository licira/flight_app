import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

/**
 * Object to write Datasets to CSV files.
 */
object CsvWriter {

  implicit class DataFrameOps(df: DataFrame) {

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

  def writeToCsv(df: DataFrame,
                 path: String,
                 header: Boolean = true,
                 saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    df.coalesce(1)
      .write
      .mode(saveMode)
      .option("header", header.toString)
      .csv(path)
  }

  def toCsvStructure[T](ds: Dataset[T]): DataFrame = {
    ds match {
      case _: Dataset[FrequentFlyer] =>
        ds.withColumnRenamed("passengerId", "Passenger ID")
          .withColumnRenamed("flightCount", "Flight Count")
      case _: Dataset[FrequentFlyerWithPassengerDetails] =>
        ds.withColumnRenamed("passengerId", "Passenger ID")
          .withColumnRenamed("flightCount", "Flight Count")
          .withColumnRenamed("firstName", "First Name")
          .withColumnRenamed("lastName", "Last Name")
      case _: Dataset[LongestRun] =>
        ds.withColumnRenamed("passengerId", "Passenger ID")
          .withColumnRenamed("longestRun", "Longest Run")
      case _: Dataset[FlightsTogether] =>
        ds.withColumnRenamed("passengerId1", "Passenger 1 ID")
          .withColumnRenamed("passengerId2", "Passenger 2 ID")
          .withColumnRenamed("flightsTogether", "Number of Flights Together")
      case _: Dataset[FlightsTogetherBetween] =>
        ds.select(col("passengerId1").as("Passenger 1 ID"),
          col("passengerId2").as("Passenger 2 ID"),
          col("flightsTogether").as("Number of Flights Together"),
          col("from").as("From"),
          col("to").as("To"))
    }
  }
}