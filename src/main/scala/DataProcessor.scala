import DataProcessor.{generatePassengerPairs, reducefc, stringToDate}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Date
import java.text.SimpleDateFormat

/**
 * Class with computational methods for datasets.
 *
 * @param spark SparkSession.
 */
class DataProcessor(spark: SparkSession) {

  /**
   * Computes the most frequent flyers.
   *
   * @param flights dataset of type Flight.
   * @param n       The number of top frequent flyers to retrieve.
   * @return A Dataset of FrequentFlyerWithPassengerDetails representing the most frequent flyers.
   */
  def mostFrequentFlyers(flights: Dataset[Flight],
                         passengers: Dataset[Passenger],
                         n: Int): Dataset[FrequentFlyerWithPassengerDetails] = {
    import flights.sparkSession.implicits._

    val mostFrequentPassengerIds = flights.groupByKey(_.passengerId)
      .mapGroups((passengerId, flights) => FrequentFlyer(passengerId, flights.size))
      .orderBy(desc("flightCount"))
      .limit(n)

    mostFrequentPassengerIds.join(passengers, Seq("passengerId"))
      .select($"passengerId".as[Int],
        $"flightCount".as[Long],
        $"firstName".as[String],
        $"lastName".as[String])
      .as[FrequentFlyerWithPassengerDetails]
      .orderBy(desc("flightCount"))
  }

  /**
   * Computes the count of flights per month.
   *
   * @param flights dataset of type Flight.
   * @return A Dataset of FlightCount representing the count of flights per month.
   */
  def countFlightsByMonth(flights: Dataset[Flight]): Dataset[FlightCount] = {
    import flights.sparkSession.implicits._

    flights.map { flight =>
        val month = flight.date.substring(0, 7) // Extract year-month part from the date
        FlightCount(month, flight.flightId)
      }.groupByKey(_.month)
      .flatMapGroups((month, flights) =>
        Iterator.single(
          FlightCount(month.substring(5, 7).toInt.toString,
            flights.toSet.size))) // Extract month part from the year-month
      .orderBy(desc("count"))
  }

  /**
   * Computes the longest run of flights bypassing a specified country.
   *
   * @param flights dataset of type Flight.
   * @param country The country to be bypassed.
   * @return A Dataset of LongestRun representing the longest run of flights bypassing the specified country.
   */
  def longestRunBypassingCountry(flights: Dataset[Flight],
                                 country: String): Dataset[LongestRun] = {
    import flights.sparkSession.implicits._

    flights.groupByKey(_.passengerId)
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
   * @param flights    dataset of type Flight.
   * @param minFlights The minimum number of flights together.
   * @return A Dataset of FlightsTogether representing the pairs of passengers and the number of flights they were on together.
   */
  def minimumCoFlightsByPassengers(flights: Dataset[Flight],
                                   minFlights: Int): Dataset[FlightsTogether] = {
    import flights.sparkSession.implicits._

    // Self-join equivalent operation to find pairs of passengers on the same flight
    flights.groupByKey(_.flightId)
      .flatMapGroups((_, flights) => generatePassengerPairs(flights))
      .groupByKey(t => (t._1, t._2))
      .mapGroups {
        case (t, c) => FlightsTogether(t._1, t._2, c.size)
      }.filter(_.flightsTogether > minFlights)
      .orderBy(desc("flightsTogether"))
  }

  /**
   * Computes the pairs of passengers who have been on more than a specified number of flights together within a given date range.
   *
   * @param flights    dataset of type Flight.
   * @param minFlights The minimum number of flights together.
   * @param from       The start date of the range.
   * @param to         The end date of the range.
   * @return A Dataset of FlightsTogetherBetween representing the pairs of passengers, the number of flights they were on together, and the date range.
   */
  def minimumCoFLightsByPassengersBetweenDates(flights: Dataset[Flight],
                                               minFlights: Int,
                                               from: Date,
                                               to: Date): Dataset[FlightsTogetherBetween] = {
    import flights.sparkSession.implicits._

    // Get flights withing dates.
    val flightsWithinInterval = withinInterval(flights, from, to)

    // Passenger pairs from flights within dates.
    val pairs = passengerPairs(flightsWithinInterval)

    // Map passenger pairs to flights that they travelled on together.
    val passengerPairFlights = passengerPairsToFlights(pairs, minFlights)
      .orderBy(desc("flightsTogether"))

    passengerPairFlights
  }

  private def passengerPairsToFlights(pairs: Dataset[(Int, Int, String, String)],
                                      minFlights: Int) = {
    import pairs.sparkSession.implicits._

    // Group by coflyer passengers.
    pairs.groupByKey { case (passenger1, passenger2, _, _) => (passenger1, passenger2) }
      .flatMapGroups { case ((passenger1, passenger2), flights) =>
        val flightsList = flights.toList
        // If passengers have been flying together more than minFlights return this information as a FlightsTogetherBetween.
        if (flightsList.size > minFlights) {

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
      }
  }

  private def withinInterval(flights: Dataset[Flight],
                             from: Date,
                             to: Date): Dataset[Flight] = {
    import flights.sparkSession.implicits._

    flights.flatMap { flight =>
      val parsedDate = stringToDate(flight.date)

      if ((parsedDate.after(from) || !parsedDate.before(from)) &&
        (parsedDate.before(to) || !parsedDate.after(to))) {
        Iterator.single(flight)
      } else {
        Iterator.empty
      }
    }
  }

  private def passengerPairs(flights: Dataset[Flight]): Dataset[(Int, Int, String, String)] = {
    import flights.sparkSession.implicits._

    flights.groupByKey(_.flightId)
      .flatMapGroups((_, flights) => generatePassengerPairs(flights))
  }
}

/**
 * Companion object for DataProcessor class.
 */
object DataProcessor {

  /**
   * Creates a new instance of DataProcessor.
   *
   * @param spark The SparkSession used for processing.
   * @return A new instance of DataProcessor.
   */
  def apply(spark: SparkSession): DataProcessor = new DataProcessor(spark)

  /**
   * Helper function to convert a String to java.sql.Date.
   *
   * @param dateStr The date string to convert.
   * @return The converted java.sql.Date.
   */
  private def stringToDate(dateStr: String): Date = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    new Date(format.parse(dateStr).getTime)
  }

  /**
   * Generates pairs of passenger IDs and their corresponding dates.
   *
   * @param flights An iterator of flights for a specific flight ID.
   * @return An iterator of tuples containing passenger pairs and their dates.
   */
  private def generatePassengerPairs(flights: Iterator[Flight]): Iterator[(Int, Int, String, String)] = {
    val flightsBuffer = flights.toBuffer
    for {
      i <- flightsBuffer.indices.iterator
      j <- (i + 1) until flightsBuffer.length
    } yield {
      if (flightsBuffer(i).passengerId < flightsBuffer(j).passengerId) {
        (flightsBuffer(i).passengerId, flightsBuffer(j).passengerId, flightsBuffer(i).date, flightsBuffer(j).date)
      } else {
        (flightsBuffer(j).passengerId, flightsBuffer(i).passengerId, flightsBuffer(j).date, flightsBuffer(i).date)
      }
    }
  }

  /**
   * Reduce function to find the minimum "from" date and the maximum "to" date directly from flights.
   */
  private val reducefc: ((Date, Date), (Date, Date)) => (Date, Date) = {
    case ((minFrom1, maxTo1), (minFrom2, maxTo2)) =>
      (
        if (minFrom1.before(minFrom2)) minFrom1 else minFrom2,
        if (maxTo1.after(maxTo2)) maxTo1 else maxTo2
      )
  }
}