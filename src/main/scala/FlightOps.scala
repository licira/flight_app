import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Date
import java.text.SimpleDateFormat

object FlightOps {

  implicit class FlightDatasetOps(ds: Dataset[Flight]) {

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
        .agg(count("*").as[Long].alias("count"))
        .select("month", "count")
        .orderBy("month")
        .as[FlightCount]
    }

    def computeLongestRunBypassingCountry(country: String): Dataset[LongestRun] = {
      import ds.sparkSession.implicits._

      val flightsNotInCountry = ds.filter($"from" =!= country && $"to" =!= country)
        .orderBy("passengerId", "date")

      flightsNotInCountry.groupByKey(_.passengerId)
        .mapGroups { case (passengerId, flights) =>
          var maxRun = 0
          var currentRun = 0
          var lastFlight: Option[Flight] = None

          flights.foreach { flight =>
            if (lastFlight.isEmpty || flight.flightId != lastFlight.get.flightId) {
              currentRun += 1
              maxRun = math.max(maxRun, currentRun)
            } else {
              currentRun = 1
            }
            lastFlight = Some(flight)
          }
          LongestRun(passengerId, maxRun)
        }.orderBy(desc("longestRun"))
    }

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
