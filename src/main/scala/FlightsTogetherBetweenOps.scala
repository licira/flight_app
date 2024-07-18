import org.apache.spark.sql.Dataset

object FlightsTogetherBetweenOps {

  implicit class FlightsTogetherBetweenDatasetOps(ds: Dataset[FlightsTogetherBetween]) {

    def writeToCsv(path: String): Unit = {
      import ds.sparkSession.implicits._

      ds.select($"passengerId1".as("Passenger 1 ID").as[String],
          $"passengerId2".as("Passenger 2 ID").as[String],
          $"flightsTogether".as("Number of flights together").as[String],
          $"from".as("From").as[String],
          $"to".as("To").as[String])
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(path)
    }
  }
}
