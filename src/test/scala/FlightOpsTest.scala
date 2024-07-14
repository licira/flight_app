import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import FlightOps._
import org.apache.spark.sql.functions._

class FlightOpsTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("FlightOpsTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  "flightsPerMonth" should "compute the correct flight counts per month" in {
    val flights = Seq(
      Flight(1, 1, "NYC", "LAX", "2023-01-01"),
      Flight(2, 2, "LAX", "NYC", "2023-01-02"),
      Flight(3, 3, "NYC", "LAX", "2023-02-01"),
      Flight(4, 4, "LAX", "NYC", "2023-02-02")
    ).toDS()

    val result = flights.countFlightsByMonth().collect()

    result should contain theSameElementsAs Seq(
      FlightCount("1", 2),
      FlightCount("2", 2)
    )
  }
}
