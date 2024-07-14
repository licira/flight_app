import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, StringType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CsvReaderTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("CsvReaderTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  "readCsv" should "correctly read a CSV file into a DataFrame" in {
    // Define the schema
    val schema = StructType(Array(
      StructField("passengerId", IntegerType, nullable = false),
      StructField("flightId", IntegerType, nullable = false),
      StructField("from", StringType, nullable = false),
      StructField("to", StringType, nullable = false),
      StructField("date", StringType, nullable = false)
    ))

    // Create a temporary CSV file
    val tempFile = java.io.File.createTempFile("test", ".csv")
    val writer = new java.io.PrintWriter(tempFile)
    try {
      writer.write(
        """passengerId,flightId,from,to,date
          |1,1,NYC,LAX,2023-01-01
          |2,2,LAX,NYC,2023-01-02
          |3,3,NYC,LAX,2023-02-01
          |4,4,LAX,NYC,2023-02-02""".stripMargin)
    } finally {
      writer.close()
    }

    // Read the CSV file
    val df = CsvReader.readCsv(spark, tempFile.getAbsolutePath, schema)

    // Expected data
    val expectedData = Seq(
      Row(1, 1, "NYC", "LAX", "2023-01-01"),
      Row(2, 2, "LAX", "NYC", "2023-01-02"),
      Row(3, 3, "NYC", "LAX", "2023-02-01"),
      Row(4, 4, "LAX", "NYC", "2023-02-02")
    )

    // Convert DataFrame to a sequence of Rows
    val resultData = df.collect()

    // Assert the DataFrame contents
    resultData should contain theSameElementsAs expectedData

    // Delete the temporary file
    tempFile.delete() should be (true)
  }
}
