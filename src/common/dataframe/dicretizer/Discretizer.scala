package common.dataframe.dicretizer

import org.apache.spark.sql.DataFrame

trait Discretizer {
  def discretize(data: DataFrame): DataFrame

  def discretize(data: DataFrame, cols: Array[String]): DataFrame
}
