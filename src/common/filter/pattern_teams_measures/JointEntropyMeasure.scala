package common.filter.pattern_teams_measures

import common.utils.PatternToBinaryFeature

class JointEntropyMeasure {

  def getMeasure(patterns: Array[Int], matrixMaker: PatternToBinaryFeature): Double = {
    var sum = 0d
    for (i ← 0 until matrixMaker.rowsCount) {
      val positive = patterns.map(p ⇒ matrixMaker.matrix(p)(i)).count(bool ⇒ bool)
      sum += (positive / matrixMaker.rowsCount) * math.log(positive / matrixMaker.rowsCount)
    }
    sum
  }
}
