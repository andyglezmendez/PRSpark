package common.filter.pattern_teams_measures

import common.utils.{BitArray, PatternToBinaryFeature}

class ExclusiveCoverageMeasure {

  def getMeasure(patterns: Array[Int], matrixMaker: PatternToBinaryFeature): Double = {
    val matrix = matrixMaker.matrix
    var sum = 0d
    for (pattern ← patterns){
      var union = new BitArray(matrixMaker.rowsCount)
      union.setAllValues(false)
      patterns.foreach(p ⇒ if(p != pattern) union = union or matrix(p))

      val c = matrix(pattern).and(union).count(bool => bool)
      val s = matrix(pattern).count(bool => bool)
      sum += s - c
    }
    sum
  }

}
