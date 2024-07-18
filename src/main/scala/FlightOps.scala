import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

import java.sql.Date
import java.text.SimpleDateFormat

/**
 * Object containing operations for processing flight data.
 */
object FlightOps {

  /**
   * Implicit class to add operations to Dataset[Flight].
   *
   * @param ds The Dataset[Flight] to be extended.
   */
  implicit class FlightDatasetOps(ds: Dataset[Flight]) {

    /**
     * Computes the most frequent flyers.
     *
     * @param n The number of top frequent flyers to retrieve.
     * @return A Dataset of FrequentFlyer representing the most frequent flyers.
     */
    def computeMostFrequentFlyers(n: Int): Dataset[FrequentFlyer] = {
      import ds.sparkSession.implicits._

      ds.groupBy("passengerId")
        .count()
        .orderBy(desc("count"))
        .limit(n)
        .select($"passengerId", $"count".as("flightCount"))
        .as[FrequentFlyer]
    }

    /**
     * Computes the count of flights per month.
     *
     * @return A Dataset of FlightCount representing the count of flights per month.
     */
    def countFlightsByMonth(): Dataset[FlightCount] = {
      import ds.sparkSession.implicits._

      ds.withColumn("month", month($"date"))
        .withColumn("year", month($"date"))
        .groupBy("year", "month")
        .agg(countDistinct("flightId").as[Long].alias("count"))
        .select("month", "count")
        .orderBy(desc("count"))
        .as[FlightCount]
    }

    /**
     * Computes the longest run of flights bypassing a specified country.
     *
     * @param country The country to be bypassed.
     * @return A Dataset of LongestRun representing the longest run of flights bypassing the specified country.
     */
    def computeLongestRunBypassingCountry(country: String): Dataset[LongestRun] = {
      import ds.sparkSession.implicits._

      ds.groupByKey(_.passengerId)
        .flatMapGroups { case (passengerId, flights) =>
          val longestConsecutiveRunSkippingCountry = flights.map(f => (f.from, f.to))
            .flatMap { case (from, to) => Seq(from, to) }
            .foldLeft((List.empty[String], 0, 0)) {
              case ((acc, maxRun, currentRun), currentCountry) =>
                if (acc.isEmpty || acc.last != currentCountry) {
                  if (currentCountry != country) {
                    val newRun = currentRun + 1
                    (acc :+ currentCountry, math.max(maxRun, newRun), newRun)
                  } else {
                    (acc :+ currentCountry, maxRun, 0)
                  }
                } else {
                  (acc, maxRun, currentRun)
                }
            }._2
          Iterator(LongestRun(passengerId, longestConsecutiveRunSkippingCountry))
        }.orderBy(desc("longestRun"))
    }

    /**
     * Computes the pairs of passengers who have been on more than a specified number of flights together.
     *
     * @param minFlights The minimum number of flights together.
     * @return A Dataset of FlightsTogether representing the pairs of passengers and the number of flights they were on together.
     */
    def computesMinimumCoFlightsByPassengers(minFlights: Int): Dataset[FlightsTogether] = {
      import ds.sparkSession.implicits._

      // Self-join to find pairs of passengers on the same flight


      val joined = ds.as("df1")
        .join(ds.as("df2"), $"df1.flightId" === $"df2.flightId"
          && $"df1.passengerId" < $"df2.passengerId")
        .select($"df1.passengerId".alias("passenger1"),
          $"df2.passengerId".alias("passenger2"))

      // Group by the pairs of passengers and count the number of flights together
      joined.groupBy("passenger1", "passenger2")
        .agg(count("*").alias("flightCount"))
        .filter(col("flightCount") > minFlights)
        .select(col("passenger1").as("passengerId1"),
          col("passenger2").as("passengerId2"),
          col("flightCount").as("flightsTogether"))
        .as[FlightsTogether]
        .orderBy(desc("flightsTogether"))
    }

    /**
     * Computes the pairs of passengers who have been on more than a specified number of flights together within a given date range.
     *
     * @param minFlights The minimum number of flights together.
     * @param from       The start date of the range.
     * @param to         The end date of the range.
     * @return A Dataset of FlightsTogetherBetween representing the pairs of passengers, the number of flights they were on together, and the date range.
     */
    def computeMinimumCoFLightsByPassengersBetweenDates(minFlights: Int, from: Date, to: Date): Dataset[FlightsTogetherBetween] = {
      import ds.sparkSession.implicits._

      // Helper function to convert String to java.sql.Date
      def stringToDate(dateStr: String): Date = {
        val format = new SimpleDateFormat("yyyy-MM-dd")
        new Date(format.parse(dateStr).getTime)
      }

      // Create Dataset[FlightWithParsedDate]
      val flightsWithinInterval =
        ds.map { flight =>
            FlightWithParsedDate(flight.passengerId, flight.flightId, flight.from, flight.to, flight.date, stringToDate(flight.date))
          }.filter(flight => (flight.parsedDate.after(from) || !flight.parsedDate.before(from)) &&
            (flight.parsedDate.before(to) || !flight.parsedDate.after(to)))
          .map(flight => Flight(flight.passengerId, flight.flightId, flight.from, flight.to, flight.date))

      val joined = flightsWithinInterval.as("df1")
        .join(ds.as("df2"), $"df1.flightId" === $"df2.flightId"
          && $"df1.passengerId" < $"df2.passengerId")
        .select(
          col("df1.passengerId").as("passengerId1"),
          col("df2.passengerId").as("passengerId2"),
          col("df1.date"))

      joined.groupBy("passengerId1", "passengerId2")
        .agg(count("*").alias("flightCount"),
          min("date").alias("From"),
          max("date").alias("To"))
        .filter(col("flightCount") > minFlights)
        .select(col("passengerId1"),
          col("passengerId2"),
          col("flightCount").alias("flightsTogether"),
          col("From"),
          col("To")
        )
        .as[FlightsTogetherBetween]
    }
  }
}
