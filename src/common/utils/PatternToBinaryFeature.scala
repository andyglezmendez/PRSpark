package common.utils

import common.feature.CategoricalFeature
import common.miner.DatasetSchema
import common.pattern.ContrastPattern
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{BooleanType, StructField, StructType}


class PatternToBinaryFeature(val patterns: Array[ContrastPattern], val rows: Array[Row], dataMiner: DatasetSchema) {

  /**
    * Binary matrix that represents patterns like
    * binary feature matching rows
    */
  val matrix: Array[BitArray] = buildMatrix()
  val patternsCount: Int = patterns.length
  val rowsCount: Int = rows.length
  val classFeatures: Array[(CategoricalFeature, Int)] = dataMiner.classFeatures()
  val classByPattern: Array[Int] = patterns.map(pattern ⇒ classFeatures.indexWhere(tuple ⇒ pattern.clazz == tuple._1))
  val classByRow: Array[Int] = rows.map(row ⇒ classFeatures.indexWhere(tuple ⇒ tuple._1.isMatch(row)))

  private def buildMatrix(): Array[BitArray] = {
    val bitSets = new Array[BitArray](patterns.length)

    for (i ← patterns.indices) {
      bitSets(i) = new BitArray(rows.length)
      for (j ← rows.indices) {
        if (patterns(i).isMatch(rows(j))) bitSets(i)(j) = true
        else bitSets(i)(j) = false
      }
    }
    bitSets
  }

  def instancesToBinaryRowsFromPatterns(patternSet: Array[Int]): Array[Row] = {
    val fields = new Array[StructField](patternSet.length)
    for (i ← patternSet)
      fields(i) = new StructField(i.toString, BooleanType, false)
    val schema = new StructType(fields)

    val newRows = new Array[Row](rowsCount)
    for (i ← 0 until rowsCount) {
      val values: Array[Any] = patternSet.map(p ⇒ matrix(p)(i))
      newRows(i) = new GenericRowWithSchema(values, schema)
    }

    newRows
  }

  def getPatterns(patternSet: Array[Int]): Array[ContrastPattern] = {
    patternSet.map(i ⇒ patterns(i))
  }

  def getBitArrayFromPatterns(patterns: Array[Int]): Array[BitArray] = {
    patterns.map(p ⇒ matrix(p))
  }

  /**
    * Return index of patterns that belong to the class clazz
    * @param clazz Pattern's class
    * @return Index of patterns
    */
  def getPatternsFromClass(clazz: Int): Array[Int] = {
    (0 until patternsCount).toArray.filter(i ⇒ classByPattern(i) == clazz)
  }

  /**
    * Return index of rows that belong to the class clazz
    * @param clazz Class, label of instances
    * @return Index of rows
    */
  def getRowsFromClass(clazz: Int): Array[Int] = {
    (0 until rowsCount).toArray.filter(i ⇒ classByRow(i) == clazz)
  }
}
