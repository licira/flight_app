import org.apache.spark.sql.Dataset

object FlightsTogetherOps {

  implicit class FlightsTogetherDatasetOps(ds: Dataset[FlightsTogether]) {

    def writeToCsv(path: String): Unit = {
      import ds.sparkSession.implicits._

      ds.select($"passengerId1".as("Passenger 1 ID").as[String],
          $"passengerId2".as("Passenger 2 ID").as[String],
          $"flightsTogether".as("Number of flights together").as[String])
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(path)
    }
  }
}
