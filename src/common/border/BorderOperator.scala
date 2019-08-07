package common.border

import common.feature.CategoricalFeature
import common.miner.DatasetSchema
import common.pattern.ContrastPattern

import scala.collection.mutable.ArrayBuffer

/**
  * Calc borders
  */
object BorderOperator extends Serializable {

  /**
    * Extract all borders in patterns array
    * @param patterns Array of patterns
    * @param dataMiner Used to divide the patterns in class groups on an effective way
    * @return Array of (left, right). Left is an array of minimal pattern and right is
    *         an array of maximal patterns
    */
  def allBorders(patterns: Array[ContrastPattern], dataMiner: DatasetSchema): Array[(Array[ContrastPattern], Array[ContrastPattern])] = {
    val featureIndex = dataMiner.columnsCount - 1
    val featureName = dataMiner.columnsInfo(featureIndex).name
    val classFeatures = dataMiner.valuesWithCount(featureIndex)
      .map(pair ⇒ new CategoricalFeature(featureName, featureIndex, pair._1))

    allBorders(patterns, classFeatures)
  }

  /**
    * Extract all borders in patterns array
    * @param patterns Array of patterns
    * @param classFeatures Used to divide the patterns in class groups on an effective way
    * @return Array of (left, right). Left is an array of minimal pattern and right is
    *         an array of maximal patterns
    */
  def allBorders(patterns: Array[ContrastPattern], classFeatures: Array[CategoricalFeature]): Array[(Array[ContrastPattern], Array[ContrastPattern])] = {
    val groupPatterns = new Array[ArrayBuffer[ContrastPattern]](classFeatures.length)
    for(i ← groupPatterns.indices){
      groupPatterns(i) = new ArrayBuffer[ContrastPattern]()
    }

    patterns.foreach(pattern ⇒ groupPatterns(classFeatures.indexWhere(_ == pattern.clazz)) += pattern)
    val borders = new Array[(Array[ContrastPattern], Array[ContrastPattern])](classFeatures.length)

    for (i ← groupPatterns.indices) {
      var right = new ArrayBuffer[ContrastPattern]
      var left = new ArrayBuffer[ContrastPattern]

      for (pattern ← groupPatterns(i)) {
        val complement = new ArrayBuffer[ContrastPattern]()
        for (j ← groupPatterns.indices) {
          if (j != i) complement ++= groupPatterns(j)
        }
        var leftBorder = borderDiff(pattern, complement.toArray)
        if (leftBorder.isDefined) {
          right += pattern
          for (patt ← leftBorder.get){
            if(!left.exists(p ⇒ p.id == patt.id))
              left += patt
          }

//          left = left.union(leftBorder.get.toSet)
        }
      }
      if (left.nonEmpty && right.nonEmpty)
        borders(i) = (left.toArray,right.toArray)
    }
    borders
  }

  /**
    * Calc left borders to right pattern if right don't is sub-pattern of any in patterns
    *
    * @param right      Pattern to extract left borders
    * @param complement Patterns with class !=  right.clazz
    * @return left borders patterns of right, else None
    */
  def borderDiff(right: ContrastPattern, complement: Array[ContrastPattern]): Option[Array[ContrastPattern]] = {
    if (complement.exists(pattern ⇒ right.isSubPatternOf(pattern))) return None
    Some(cartesianProduct(diff(right, complement)))
  }

  /**
    * Calc difference between rigth pattern and all patterns in patterns
    *
    * @param right
    * @param patterns
    * @return Difference of right with all in patterns
    */
  def diff(right: ContrastPattern, patterns: Array[ContrastPattern]): Array[ContrastPattern] = {
    patterns.map(pattern ⇒ {
      val features = right.predicate.diff(pattern.predicate)
      new ContrastPattern(features, right.clazz)
    })
  }

  /**
    * Cartesian product between all patterns in diffPatterns
    *
    * @param diffPatterns
    * @return Cartesian products of minimal diff patterns
    */
  def cartesianProduct(diffPatterns: Array[ContrastPattern]): Array[ContrastPattern] = {
    var left = diffPatterns.head.predicate.map(feature ⇒ new ContrastPattern(Array(feature), diffPatterns.head.clazz))

    for (i ← 1 until diffPatterns.length) {
      var tempList = new ArrayBuffer[ContrastPattern]()

      for (pattern ← left) {
        for (feature ← diffPatterns(i).predicate) {
          var predicate = pattern.predicate
          if (!predicate.contains(feature))
            predicate ++= Array(feature)
          removeNonMinimal(new ContrastPattern(predicate, pattern.clazz), tempList)
        }
      }
      left = tempList.toArray
    }
    left
  }

  /**
    * Remove non-minimal patterns in left compared with newPattern
    *
    * @param newPattern The new patter to add
    * @param left       List of minimal pattern
    */
  def removeNonMinimal(newPattern: ContrastPattern, left: ArrayBuffer[ContrastPattern]): Unit = {
    val orderedLeft = left.sortBy(pattern ⇒ pattern.predicate.length)

    for (pattern ← orderedLeft) {
      if (pattern.predicate.length < newPattern.predicate.length) {
        if (newPattern.isSuperPatternOf(pattern)) return
      }
      else {
        if (pattern.isSuperPatternOf(newPattern)) left.remove(left.indexOf(pattern))
      }
    }
    left += newPattern
  }

}