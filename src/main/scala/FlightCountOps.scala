import org.apache.spark.sql.Dataset

object FlightCountOps {

  implicit class FlightCountDatasetOps(ds: Dataset[FlightCount]) {

    def writeToCsv(path: String): Unit = {
      import ds.sparkSession.implicits._

      ds.select($"month".as("Month").as[String],
          $"count".as("Number of Flights").as[String])
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(path)
    }
  }
}
