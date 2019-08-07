package common.filter

import common.filter.chosen_few_measures.{ChosenFewMeasure, PartitionSizeQuotient}
import common.miner.DatasetSchema
import common.pattern.ContrastPattern
import common.utils.{BitArray, PatternToBinaryFeature}
import org.apache.spark.sql.Row

class ChosenFewSetConstraint(val rows: Array[Row]) {

  var order: (ContrastPattern ⇒ Double) = {_ ⇒ 1}
  private var matrixMaker: PatternToBinaryFeature = _
  var measure: ChosenFewMeasure = new PartitionSizeQuotient()

  def getPatternSet(patterns: Array[ContrastPattern], threshold: Double): Array[ContrastPattern] = {
    val ordered = orderPatterns(patterns)
    val matrixMaker = new PatternToBinaryFeature(ordered, rows, new DatasetSchema(rows))
    val matrix = matrixMaker.matrix

    var patternSet = Array(0)
    var equivalence = ChosenFewSetConstraint.equivalenceRelation(patternSet, matrix)

    for (i ← 1 until matrixMaker.patternsCount) {
      val newPatternSet = patternSet :+ i
      val newEquivalence = ChosenFewSetConstraint.equivalenceRelation(newPatternSet, matrix)

      val measureValue = measure.getMeasure(patternSet, newPatternSet, matrixMaker)
      if (newEquivalence > equivalence && measureValue > threshold) {
        patternSet = newPatternSet
        equivalence = newEquivalence
      }
    }

    patternSet.map(i ⇒ patterns(i))
  }

  private def orderPatterns(patterns: Array[ContrastPattern]): Array[ContrastPattern] = {
    patterns.sortBy(order)
  }

}

object ChosenFewSetConstraint {
  def equivalenceRelation(patternSet: Array[Int], matrix: Array[BitArray]): Int = {
    var matching = matrix(patternSet(0))
    for (i ← 1 until patternSet.length)
      matching = matching or matrix(i)
    matching.count(bool ⇒ bool)
  }
}