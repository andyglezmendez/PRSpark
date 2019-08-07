package common.miner

import common.feature.{CategoricalFeature, DoubleFeature, Feature, IntegerFeature}
import org.apache.spark.mllib.tree.configuration.FeatureType
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

//TODO Si recibe el dataset debe recorrer todos los datos feature_count veces
//TODO Si recibe un array de rows puede ser mas eficiente el codigo, pero la informacion sera local no global
/**
  * DatasetSchema hold information about a DataSet. It is used
  * to run miners.
  */
class DatasetSchema private() extends Serializable {

  /**
    * An array of StructField. Each ones represents information about a column.
    * name: Column name
    * dataType: Column DataType
    * nullable: If can be null (Spark property)
    */
  var columnsInfo: Array[StructField] = _

  /**
    * Every position in the array correspond to a column. In every position
    * are an array of (String, Int) that represent (value, count(value)) of
    * every possible value in the column
    */
  var valuesWithCount: Array[Array[(String, Int)]] = _

  var columnsCount = 0

  var instanceCount = 0L

  /**
    * DatasetSchema hold information about a DataSet. It is used
    * to run miners.
    *
    * @param dataset Dataset[Row] to mine
    */
  def this(dataset: Dataset[Row]) {
    this()
    columnsInfo = dataset.schema.fields
    valuesWithCount = new Array[Array[(String, Int)]](columnsInfo.length)
    columnsCount = columnsInfo.length
    instanceCount = dataset.count()
    extractColumnInfo(dataset)
  }

  /**
    * DatasetSchema hold information about a DataSet. It is used
    * to run miners.
    *
    * @param rows Array of Row
    */
  def this(rows: Array[Row]) {
    this()
    columnsInfo = rows(0).schema.fields
    valuesWithCount = new Array[Array[(String, Int)]](columnsInfo.length)
    columnsCount = columnsInfo.length
    instanceCount = rows.length
    extractColumnInfo(rows)
  }

  private def extractColumnInfo(dataset: Dataset[Row]): Unit = {
    dataset.createOrReplaceTempView("ds")
    val sQLContext = dataset.sqlContext

    for (i ← columnsInfo.indices) {
      val col = columnsInfo(i)
      var sqlQuery = ""

      col.dataType match {
          //TODO Fix sql query for strange characters
        case StringType ⇒ sqlQuery = s"SELECT ${col.name} as value, COUNT(${col.name}) as count FROM ds GROUP BY ${col.name}"
        case IntegerType | DoubleType ⇒ sqlQuery = s"SELECT MAX(${col.name}) as max_value, MIN(${col.name}) as min_value FROM ds"
        case _ ⇒ throw new IllegalArgumentException("Type not supported")
      }

      val rows: Array[Row] = sQLContext.sql(sqlQuery).collect()

      col.dataType match {
        case StringType ⇒ valuesWithCount(i) = rows.map(row ⇒ (row.get(0).toString, row.getLong(1).toInt))
        case IntegerType | DoubleType ⇒ valuesWithCount(i) = Array((rows(0).get(0).toString, 1), (rows(0).get(1).toString, 1))
      }
    }
  }

  private def extractColumnInfo(rows: Array[Row]): Unit = {
    val tempNominalValues = new mutable.ListMap[Int, ArrayBuffer[(String, Int)]]
    val tempDoubleValues = new mutable.ListMap[Int, ArrayBuffer[Double]]
    val tempIntegerValues = new mutable.ListMap[Int, ArrayBuffer[Int]]

    for (row ← rows; i ← columnsInfo.indices) {
//      val value = row.get(i)
      columnsInfo(i).dataType match {
        case v: StringType ⇒ categoricalColumnInfo1(row.get(i).asInstanceOf[String], tempNominalValues, i)
        case v: IntegerType ⇒ numericIntegerColumnInfo1(row.get(i).asInstanceOf[Int], tempIntegerValues, i)
        case v: DoubleType ⇒ numericDoubleColumnInfo1(row.get(i).asInstanceOf[Double], tempDoubleValues, i)
        case _ ⇒ throw new IllegalArgumentException("Type not supported")
      }
//      value match {
//        case v: String ⇒ categoricalColumnInfo(v, tempNominalValues, i)
//        case v: Int ⇒ numericIntegerColumnInfo(v, tempIntegerValues, i)
//        case v: Double ⇒ numericDoubleColumnInfo(v, tempDoubleValues, i)
//        case null ⇒ ;
//        case _ ⇒ throw new IllegalArgumentException("Type not supported")
//      }
    }

    for (i ← columnsInfo.indices) {
      columnsInfo(i).dataType match {
        case StringType ⇒ valuesWithCount(i) = tempNominalValues(i).toArray
        case IntegerType ⇒ valuesWithCount(i) = tempIntegerValues(i).map(value ⇒ (value.toString, 1)).toArray
        case DoubleType ⇒ valuesWithCount(i) = tempDoubleValues(i).map(value ⇒ (value.toString, 1)).toArray
      }
    }
  }

  private def numericDoubleColumnInfo(value: Double, tempValues: mutable.ListMap[Int, ArrayBuffer[Double]], index: Int): Unit = {
    if (tempValues.get(index).isEmpty)
      tempValues += Tuple2(index, new ArrayBuffer[Double](2))
    if (tempValues(index).length != 2)
      tempValues(index) += value
    else {
      val numValues = tempValues(index) ++ Array(value)
      val sortedValues = numValues.sorted
      tempValues(index).clear()
      tempValues(index) += sortedValues.head
      tempValues(index) += sortedValues.last
    }
  }

  private def numericIntegerColumnInfo(value: Int, tempValues: mutable.ListMap[Int, ArrayBuffer[Int]], index: Int): Unit = {
    if (tempValues.get(index).isEmpty)
      tempValues += Tuple2(index, new ArrayBuffer[Int](2))
    if (tempValues(index).length != 2)
      tempValues(index) += value
    else {
      val numValues = tempValues(index) ++ Array(value)
      val sortedValues = numValues.sorted
      tempValues(index).clear()
      tempValues(index) += sortedValues.head
      tempValues(index) += sortedValues.last
    }
  }

  private def categoricalColumnInfo(value: String, tempValues: mutable.ListMap[Int, ArrayBuffer[(String, Int)]], index: Int): Unit = {
    if (tempValues.get(index).isEmpty)
      tempValues += Tuple2(index, new ArrayBuffer[(String, Int)])
    val valuePosition: Int = tempValues(index).indexWhere(pair ⇒ pair._1.equalsIgnoreCase(value))
    if (valuePosition != -1) {
      val pair = tempValues(index)(valuePosition)
      tempValues(index)(valuePosition) = (pair._1, pair._2 + 1)
    }
    else {
      tempValues(index).+=((value, 1))
    }
  }



  private def numericDoubleColumnInfo1(value: Double, tempValues: mutable.ListMap[Int, ArrayBuffer[Double]], index: Int): Unit = {
    if (tempValues.get(index).isEmpty)
      tempValues += Tuple2(index, new ArrayBuffer[Double](2))
    if (tempValues(index).length != 2)
      tempValues(index) += value
    else {
      val numValues = tempValues(index) ++ Array(value)
      val sortedValues = numValues.sorted
      tempValues(index).clear()
      tempValues(index) += sortedValues.head
      tempValues(index) += sortedValues.last
    }
  }

  private def numericIntegerColumnInfo1(value: Int, tempValues: mutable.ListMap[Int, ArrayBuffer[Int]], index: Int): Unit = {
    if (tempValues.get(index).isEmpty)
      tempValues += Tuple2(index, new ArrayBuffer[Int](2))
    if (tempValues(index).length != 2)
      tempValues(index) += value
    else {
      val numValues = tempValues(index) ++ Array(value)
      val sortedValues = numValues.sorted
      tempValues(index).clear()
      tempValues(index) += sortedValues.head
      tempValues(index) += sortedValues.last
    }
  }

  private def categoricalColumnInfo1(value: String, tempValues: mutable.ListMap[Int, ArrayBuffer[(String, Int)]], index: Int): Unit = {
    if (tempValues.get(index).isEmpty)
      tempValues += Tuple2(index, new ArrayBuffer[(String, Int)])
    val valuePosition: Int = tempValues(index).indexWhere(pair ⇒ pair._1 == value)
    if (valuePosition != -1) {
      val pair = tempValues(index)(valuePosition)
      tempValues(index)(valuePosition) = (pair._1, pair._2 + 1)
    }
    else {
      tempValues(index).+=((value, 1))
    }
  }

   //todo Probar este metodo
  def classFeatures(): Array[(CategoricalFeature, Int)] = {
    val clazz = valuesWithCount.last
    clazz.map { pair ⇒ (new CategoricalFeature(columnsInfo.last.name, columnsCount - 1, pair._1), pair._2) }
  }

  def features(): Array[Feature[_]] = {
    var features = ArrayBuffer.empty[Feature[_]]
    for (col ← 0 until columnsCount) {
      features ++= valuesWithCount(col).map { pair ⇒
        columnsInfo(col).dataType match {
          case StringType ⇒ new CategoricalFeature(columnsInfo(col).name, col, pair._1)
          case IntegerType ⇒ new IntegerFeature(columnsInfo(col).name, col, pair._1.toInt)
          case DoubleType ⇒ new DoubleFeature(columnsInfo(col).name, col, pair._1.toDouble)
        }
      }
    }
    features.toArray
  }

}
