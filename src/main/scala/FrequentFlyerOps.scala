import org.apache.spark.sql.Dataset

/**
 * Object containing operations for processing frequent flyer data.
 */
object FrequentFlyerOps {

  /**
   * Implicit class to add operations to Dataset[FrequentFlyer].
   * @param ds The Dataset[FrequentFlyer] to be extended.
   */
  implicit class FrequentFlyerDatasetOps(ds: Dataset[FrequentFlyer]) {

    /**
     * Joins the frequent flyer dataset with the passengers dataset to include passenger details.
     *
     * @param passengers The Dataset[Passenger] containing passenger details.
     * @return A Dataset of FrequentFlyerWithPassengerDetails representing the frequent flyers with passenger details.
     */
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
