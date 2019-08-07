package common.utils

import org.apache.spark.sql.{Column, DataFrame}

object ClassSelector {

  def selectClass(df: DataFrame, col: String): DataFrame = {
    val columns = df.columns
    val isClass = columns.last.equalsIgnoreCase(col)
    if(columns.exists(c ⇒ c.equalsIgnoreCase(col)) && !isClass) {
      val newColumns = columns.filter(!_.equalsIgnoreCase(col)) :+ col
      df.select(newColumns.head, newColumns.tail: _*)
    }
    else df
  }

}
