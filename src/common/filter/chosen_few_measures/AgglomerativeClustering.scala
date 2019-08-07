package common.filter.chosen_few_measures

import common.utils.{BitArray, PatternToBinaryFeature}

class AgglomerativeClustering extends ChosenFewMeasure {

  override def getMeasure(patternSet: Array[Int], newPatternSet: Array[Int], matrixMaker: PatternToBinaryFeature): Double = {
    1 - randIndex(patternSet, newPatternSet, matrixMaker)
  }

  private def randIndex(set: Array[Int], newSet: Array[Int], matrixMaker: PatternToBinaryFeature): Double = {
    val rowsCount = matrixMaker.rowsCount
    val matrix = matrixMaker.matrix
    var sum = 0d

    for (i ← 0 until rowsCount)
      for (j ← (i + 1) until rowsCount)
        sum += isInSameBlock(set, newSet, i, j, matrix) + isInDifferentBlock(set, newSet, i, j, matrix)
    2 * sum / (rowsCount * (rowsCount - 1))
  }

  private def isInSameBlock(set: Array[Int], newSet: Array[Int], i: Int, j: Int, matrix: Array[BitArray]): Int = {
    val setOne = set.forall(p ⇒ matrix(p)(i) == matrix(p)(j))
    val setTwo = newSet.forall(p ⇒ matrix(p)(i) == matrix(p)(j))
    if (setOne && setTwo) 1 else 0
  }

  private def isInDifferentBlock(set: Array[Int], newSet: Array[Int], i: Int, j: Int, matrix: Array[BitArray]): Int = {
    val setOne = set.exists(p ⇒ matrix(p)(i) != matrix(p)(j))
    val setTwo = newSet.exists(p ⇒ matrix(p)(i) != matrix(p)(j))
    if (setOne && setTwo) 1 else 0
  }

}
