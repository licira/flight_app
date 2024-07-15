import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.reflect.ClassTag

/**
 * Object containing methods to read CSV files into Spark Datasets with specified schemas.
 */
object CsvReader {

  /**
   * Schema for the flight CSV file.
   */
  val flightSchema: StructType = StructType(Array(
    StructField("passengerId", IntegerType, nullable = false),
    StructField("flightId", IntegerType, nullable = false),
    StructField("from", StringType, nullable = false),
    StructField("to", StringType, nullable = false),
    StructField("date", StringType, nullable = false)
  ))

  /**
   * Schema for the passenger CSV file.
   */
  val passengerSchema: StructType = StructType(Array(
    StructField("passengerId", IntegerType, nullable = false),
    StructField("firstName", StringType, nullable = false),
    StructField("lastName", StringType, nullable = false),
    StructField("age", IntegerType, nullable = true),
    StructField("gender", StringType, nullable = true)
  ))

  /**
   * Function to read a CSV file into a Spark Dataset with a specified schema.
   *
   * @tparam T The type of the Dataset (e.g., Flight or Passenger).
   * @param spark The SparkSession instance.
   * @param inputPath The path to the input CSV file.
   * @return A Dataset of type T.
   * @throws IllegalArgumentException if the type T is not supported.
   */
  def readCsv[T: Encoder : ClassTag]: (SparkSession, String) => Dataset[T] = {
    (spark: SparkSession, inputPath: String) => {
      val schema = implicitly[ClassTag[T]] match {
        case ct if ct.runtimeClass == classOf[Flight] => flightSchema
        case ct if ct.runtimeClass == classOf[Passenger] => passengerSchema
        case _ =>
          throw new IllegalArgumentException("Unsupported type")
      }
      spark.read
        .option("header", "true")
        .schema(schema)
        .csv(inputPath)
        .as[T]
    }
  }
}
