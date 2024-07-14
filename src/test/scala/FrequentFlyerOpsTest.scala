import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import FrequentFlyerOps._
import org.apache.spark.sql.functions._

class FrequentFlyerOpsTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("FrequentFlyerOpsTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  "mostFrequentFlyers" should "return the correct most frequent flyers" in {
    val flights = Seq(
      Flight(1, 1, "NYC", "LAX", "2023-01-01"),
      Flight(2, 2, "LAX", "NYC", "2023-01-02"),
      Flight(1, 3, "NYC", "LAX", "2023-01-03"),
      Flight(3, 4, "LAX", "NYC", "2023-01-04"),
      Flight(1, 5, "NYC", "LAX", "2023-01-05")
    ).toDS()

    val result = flights.mostFrequentFlyers(2).collect()

    result should contain theSameElementsAs Seq(
      FrequentFlyer(1, 3),
      FrequentFlyer(2, 1)
    )
  }
}
