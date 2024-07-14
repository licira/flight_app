import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.reflect.ClassTag

object CsvReader {

  // Define the schema for the flight CSV file
  val flightSchema: StructType = StructType(Array(
    StructField("passengerId", IntegerType, nullable = false),
    StructField("flightId", IntegerType, nullable = false),
    StructField("from", StringType, nullable = false),
    StructField("to", StringType, nullable = false),
    StructField("date", StringType, nullable = false)
  ))

  // Define the schema for the passenger CSV file
  val passengerSchema: StructType = StructType(Array(
    StructField("passengerId", IntegerType, nullable = false),
    StructField("firstName", StringType, nullable = false),
    StructField("lastName", StringType, nullable = false),
    StructField("age", IntegerType, nullable = true),
    StructField("gender", StringType, nullable = true)
  ))

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
