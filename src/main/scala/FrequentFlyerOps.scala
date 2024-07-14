import org.apache.spark.sql.Dataset

object FrequentFlyerOps {

  implicit class FrequentFlyerDatasetOps(ds: Dataset[FrequentFlyer]) {

    def joinWithPassengers(passengers: Dataset[Passenger]): Dataset[FrequentFlyerWithPassengerDetails] = {
      import ds.sparkSession.implicits._

      ds.as("frequentFlyer")
        .join(passengers.as("passenger"), $"frequentFlyer.passengerId" === $"passenger.passengerId")
        .select($"frequentFlyer.passengerId".alias("passengerId"),
          $"frequentFlyer.flightCount".alias("flightCount"),
          $"passenger.firstName".alias("firstName"),
          $"passenger.lastName".alias("lastName"))
        .as[FrequentFlyerWithPassengerDetails]
    }
  }
}
