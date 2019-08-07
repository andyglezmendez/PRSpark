package common.dataframe.dicretizer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.IntegerType

class IntegerToStringDiscretizer extends Discretizer {
  override def discretize(data: DataFrame): DataFrame = {
    var dataframe: DataFrame = data
    val toString = udf { value: Int ⇒ value.toString }

    val fields = dataframe.first().schema.fields
    for (i ← fields.indices)
      if (fields(i).dataType == IntegerType) {
        val colName = dataframe.columns(i)
        dataframe = dataframe.withColumn(colName, toString(dataframe.col(colName)))
      }
    dataframe.cache()
    dataframe
  }

  def discretize(data: DataFrame, cols: Array[String]): DataFrame = {
    var dataframe: DataFrame = data
    val toString = udf { value: Int ⇒ value.toString }

    val fields = dataframe.first().schema.fields.filter(field ⇒ cols.exists(_.equalsIgnoreCase(field.name)))
    for (i ← fields.indices)
      if (fields(i).dataType == IntegerType) {
        val colName = dataframe.columns(i)
        dataframe = dataframe.withColumn(colName, toString(dataframe.col(colName)))
      }
    dataframe.cache()
    dataframe
  }
}
