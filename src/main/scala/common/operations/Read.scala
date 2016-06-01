package common.operations

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

object Read {

  def csv(path: String, sc: SparkContext): DataFrame = {
    val sqlContext = new SQLContext(sc)

    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path).toDF()
  }
}
