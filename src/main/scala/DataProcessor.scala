import org.apache.spark.sql.{DataFrame, Dataset}

object DataProcessor {

  def compute[T](ds: Dataset[T],
                 transformations: Dataset[T] => DataFrame = (ds: Dataset[T]) => ds.toDF()): DataFrame = {
    transformations(ds)
  }
}