package common.filter.pattern_teams_measures

import common.pattern.ContrastPattern
import common.utils.PatternToBinaryFeature

class AUCMeasure {

  def getMeasure(patterns: Array[Int], matrixMaker: PatternToBinaryFeature): Double = {
    getMeasure(matrixMaker.getPatterns(patterns))
  }

  def getMeasure(patterns: Array[ContrastPattern]): Double = {
    val accOrdered = patterns.sortBy(p ⇒ p.negativeClassSupport() + p.positiveClassSupport())
    var points = (0d, 0d) +: accOrdered.map(p ⇒ (p.pp.toDouble / (p.pp + p.nn), p.np.toDouble / (p.np + p.pn)))
    points = points ++: Array((1d, 1d), (1d, 0d))
    auc(points)
  }

  def auc(curve: Iterable[(Double, Double)]): Double = {
    curve.toIterator.sliding(2).withPartial(false).aggregate(0.0)(
      seqop = (auc: Double, points: Seq[(Double, Double)]) ⇒ auc + trapezoid(points),
      combop = _ + _
    )
  }

  def trapezoid(points: Seq[(Double, Double)]): Double = {
    require(points.lengthCompare(2) == 0)
    val x = points.head
    val y = points.last
    (y._1 - x._1) * (y._2 + x._2) / 2.0
  }

}
