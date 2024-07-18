import org.apache.spark.sql.Dataset

object FrequentFlyerWithPassengerDetailsOps {

  implicit class FrequentFlyerWithPassengerDetailsDatasetOps(ds: Dataset[FrequentFlyerWithPassengerDetails]) {

    def writeToCsv(path: String): Unit = {
      import ds.sparkSession.implicits._

      ds.select($"passengerId".as("Passenger ID").as[String],
          $"flightCount".as("Flight Count").as[String],
          $"firstName".as("First Name").as[String],
          $"lastName".as("Last Name").as[String])
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(path)
    }
  }
}
