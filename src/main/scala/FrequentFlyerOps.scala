import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.desc

/**
 * Object containing operations for processing frequent flyer data.
 */
object FrequentFlyerOps {

  /**
   * Implicit class to add operations to Dataset[FrequentFlyer].
   *
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

      ds.join(passengers, Seq("passengerId"))
        .select($"passengerId".as[Int],
          $"flightCount".as[Long],
          $"firstName".as[String],
          $"lastName".as[String])
        .as[FrequentFlyerWithPassengerDetails]
        .orderBy(desc("flightCount"))
    }
  }
}
