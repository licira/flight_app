import FrequentFlyerOps.FrequentFlyerDatasetOps
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FrequentFlyerOpsSpec extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("FrequentFlyerOpsSpec")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  "topFrequentFlyers" should "correctly return the top N frequent flyers" in {
    val frequentFlyers: Dataset[FrequentFlyer] = Seq(
      FrequentFlyer(1, 10),
      FrequentFlyer(2, 20)
    ).toDS()

    val passengers: Dataset[Passenger] = Seq(
      Passenger(1, "Napoleon", "Gaylene"),
      Passenger(2, "Katherin", "Shanell"),
      Passenger(3, "Stevie", "Steven")
    ).toDS()


    val expected = Seq(
      FrequentFlyerWithPassengerDetails(1, 10, "Napoleon", "Gaylene"),
      FrequentFlyerWithPassengerDetails(2, 20, "Katherin", "Shanell")
    ).toDS()

    val actual = frequentFlyers.joinWithPassengers(passengers)

    actual.collect() should contain theSameElementsAs expected.collect()
  }
}
