import org.apache.spark.sql.Dataset

object LongestRunOps {

  implicit class LongestRunDatasetOps(ds: Dataset[LongestRun]) {

    def writeToCsv(path: String): Unit = {
      import ds.sparkSession.implicits._

      ds.select($"passengerId".as("Passenger ID").as[String],
          $"longestRun".as("Longest Run").as[String])
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(path)
    }
  }
}
