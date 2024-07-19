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

      ds.groupByKey(_.passengerId)
        .mapGroups((passengerId, flights) => FrequentFlyer(passengerId, flights.size))
        .orderBy(desc("flightCount"))
        .limit(n)
    }

    /**
     * Computes the count of flights per month.
     *
     * @return A Dataset of FlightCount representing the count of flights per month.
     */
    def countFlightsByMonth(): Dataset[FlightCount] = {
      import ds.sparkSession.implicits._

      ds.map { flight =>
          val month = flight.date.substring(0, 7) // Extract year-month part from the date
          FlightCount(month, flight.flightId)
        }.groupByKey(_.month)
        .flatMapGroups { case (month, flights) =>
          Iterator(
            FlightCount(month.substring(5, 7).toInt.toString,
              flights.toSet.size)) // Extract month part from the year-month
        }.orderBy(desc("count"))
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
      ds.groupByKey(_.flightId)
        .flatMapGroups { case (_, flights) =>
          val flightsList = flights.toList
          for {
            i <- flightsList.indices.iterator
            j <- (i + 1) until flightsList.length
          } yield {
            if (flightsList(i).passengerId < flightsList(j).passengerId) {
              (flightsList(i).passengerId, flightsList(j).passengerId)
            }
            else {
              (flightsList(j).passengerId, flightsList(i).passengerId)
            }
          }
        }.groupByKey(t => (t._1, t._2))
        .mapGroups {
          case (t, c) => FlightsTogether(t._1, t._2, c.size)
        }.filter(_.flightsTogether > minFlights)
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

      // Get flights withing dates.
      val flightsWithinInterval =
        ds.flatMap { flight =>
          val parsedDate = stringToDate(flight.date)

          if ((parsedDate.after(from) || !parsedDate.before(from)) &&
            (parsedDate.before(to) || !parsedDate.after(to))) {
            Iterator.single(flight)
          } else {
            Iterator.empty
          }
        }

      // Passenger pairs from flights within dates.
      val passengerPairs = flightsWithinInterval.groupByKey(_.flightId)
        .flatMapGroups { case (_, flights) =>
          val flightsList = flights.toList
          for {
            i <- flightsList.indices.iterator
            j <- (i + 1) until flightsList.length
          } yield {
            if (flightsList(i).passengerId < flightsList(j).passengerId) {
              (flightsList(i).passengerId, flightsList(j).passengerId, flightsList(i).date, flightsList(j).date)
            } else {
              (flightsList(j).passengerId, flightsList(i).passengerId, flightsList(j).date, flightsList(i).date)
            }
          }
        }

      implicit val dateOrdering: Ordering[Date] = Ordering.by(_.getTime)

      // Map passenger pairs to flights that they travelled on together.
      val passengerPairFlights = passengerPairs
        // Group by coflyer passengers.
        .groupByKey { case (passenger1, passenger2, _, _) => (passenger1, passenger2) }
        .flatMapGroups { case ((passenger1, passenger2), flights) =>
          val flightsList = flights.toList
          // If passengers have been flying together more than minFlights return this information as a FlightsTogetherBetween.
          if (flightsList.size > minFlights) {

            // Reduce function to find the minimum "from" date and the maximum "to" date directly from flights
            val reducefc: ((Date, Date), (Date, Date)) => (Date, Date) = {
              case ((minFrom1, maxTo1), (minFrom2, maxTo2)) =>
                (
                  if (minFrom1.before(minFrom2)) minFrom1 else minFrom2,
                  if (maxTo1.after(maxTo2)) maxTo1 else maxTo2
                )
            }

            val (minFrom, maxTo) = flightsList.map(f => (stringToDate(f._3), stringToDate(f._4)))
              .reduce(reducefc)

            Iterator.single(FlightsTogetherBetween(passenger1,
              passenger2,
              flightsList.size,
              minFrom.toString,
              maxTo.toString
            ))
          } else {
            // Discard information if passengers haven't travelled together more than minFlights.
            Iterator.empty
          }
        }.orderBy(desc("flightsTogether"))

      passengerPairFlights
    }
  }
}
