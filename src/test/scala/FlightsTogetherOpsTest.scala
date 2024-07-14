import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import FlightsTogetherOps._
import org.apache.spark.sql.functions._

class FlightsTogetherOpsTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("FlightsTogetherOpsTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  "flownTogether
  "flownTogether" should "return the passengers who have been on more than the given number of flights together" in {
    val flights = Seq(
      Flight(1, 1, "NYC", "LAX", "2023-01-01"),
      Flight(2, 1, "NYC", "LAX", "2023-01-01"),
      Flight(1, 2, "LAX", "NYC", "2023-01-02"),
      Flight(2, 2, "LAX", "NYC", "2023-01-02"),
      Flight(1, 3, "NYC", "LAX", "2023-01-03"),
      Flight(2, 3, "NYC", "LAX", "2023-01-03")
    ).toDS()

    val result = flights.flownTogether(2).collect()

    result should contain theSameElementsAs Seq(
      FlightsTogether(1, 2, 3)
    )
  }
}
