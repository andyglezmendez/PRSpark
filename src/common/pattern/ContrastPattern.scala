package common.pattern

import java.util

import common.feature.Feature
import common.quality.ContingencyTable
import org.apache.spark.sql.Row

class ContrastPattern(var predicate: Array[Feature[_]],
                      var clazz: Feature[_]) extends Serializable {
  predicate = predicate.sortBy(_.index)

  var id: Int = toString.hashCode

  /**
    * Contingency Table
    * pp -> Positive the predicate, positive the class
    * pn -> Positive the predicate, negative the class
    * np -> Negative the predicate, positive the class
    * nn -> Negative the predicate, negative the class
    */
  var pp = 0d
  var pn = 0d
  var np = 0d
  var nn = 0d

  /**
    * Map with the metrics result
    * String: Metric name
    * Double: Metric value
    */
   val metricMap = new util.HashMap[String,Double]()

  /**
    * Check for all the Features in predicate if all match
    * with the instance
    *
    * @param instance Row
    * @return True if all match, else false
    */
  def isMatch(instance: Row): Boolean = {
    predicate.forall(_.isMatch(instance))
  }

  def ==(other: ContrastPattern): Boolean = {
    if (other.clazz == clazz && other.predicate.length == predicate.length)
      if (other.predicate.forall(f ⇒ predicate.exists(f1⇒ f == f1))) return true
    false
  }

  /**
    * Calc the contingency table values
    *
    * @param instances All instances (Row)
    */
  def contingencyCalc(instances: Iterable[Row]): Unit = {
    for (instance ← instances) {
      if (predicate.forall(_.isMatch(instance))) {
        if (clazz.isMatch(instance)) pp += 1
        else pn += 1
      }
      else {
        if (clazz.isMatch(instance)) np += 1
        else nn += 1
      }
    }
  }

  def contingencyTable(): ContingencyTable = new ContingencyTable(this)

  /**
    * If this pattern is sub-pattern of other pattern
    *
    * @param other ContrastPattern to compare
    * @return True if all features in this pattern are contained in the other pattern, else false
    */
  def isSubPatternOf(other: ContrastPattern): Boolean = {
    if (predicate.length > other.predicate.length) return false
    predicate.forall(feature ⇒ other.predicate.exists(f1 ⇒ feature == f1))
  }

  /**
    * If this pattern is super-pattern of other pattern
    *
    * @param other ContrastPattern to compare
    * @return True if all features in the other pattern are contained in this pattern, else false
    */
  def isSuperPatternOf(other: ContrastPattern): Boolean = {
    if (other.predicate.length > predicate.length) return false
    other.predicate.forall(feature ⇒ predicate.exists(f1 ⇒ feature == f1))
  }

  def growthRate(): Double = {
    if (negativeClassSupport() == 0) {
      if (positiveClassSupport() == 0)
        0
      else Double.PositiveInfinity
    }
    else positiveClassSupport() / negativeClassSupport()
  }

  def positiveClassSupport() = pp.toDouble / (pp + np)
//  def positiveClassSupport() = pp.toDouble / (pp + pn + np + nn)

  def negativeClassSupport() = pn.toDouble / (pn + nn)
//  def negativeClassSupport() = pn.toDouble / (pp + pn + np + nn)

  override def toString: String = {
    var features = ""
    for (feature ← predicate) {
      features = features + s"$feature,"
    }
    if (predicate.isEmpty) return s"NONE -> $clazz"
    features = features.substring(0, features.length - 1)
    features + s" -> $clazz"
  }

  override def hashCode(): Int = id

}
