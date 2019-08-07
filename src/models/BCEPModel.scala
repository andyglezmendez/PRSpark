package models

import common.feature.{CategoricalFeature, Feature}
import common.miner.DatasetSchema
import common.model.Model
import common.pattern.ContrastPattern
import org.apache.spark.sql.Row

import scala.collection.mutable.ArrayBuffer


class BCEPModel extends Model {

  private var features: Array[Feature[_]] = Array.empty[Feature[_]]
  private var classFeatureProbability: Array[Double] = _
  private var featuresProbability: Array[ArrayBuffer[Double]] = _

  /**
    * Initialize the classifier.
    *
    * @param patterns
    * @param trainingInstances
    * @param dataMiner
    */
  override def initialize(patterns: Array[ContrastPattern], trainingInstances: Array[Row], dataMiner: DatasetSchema): Unit = {
    this.patterns = patterns
    this.trainingInstances = trainingInstances
    this.dataMiner = dataMiner
    classFeatures = dataMiner.classFeatures()
    classFeatureProbability = laplaceEstimate()
    featuresProbability = new Array(classFeatures.length)
    for (i ← featuresProbability.indices) featuresProbability(i) = new ArrayBuffer[Double]()
    pruneEP(growthRate = 150)
  }

  override def predict(instance: Row): Array[Double] = {
    val classProbability = new Array[Double](classFeatures.length)
    var matchPatterns = patterns.filter(pattern ⇒ pattern.isMatch(instance))

    var covered = Array.empty[Feature[_]]
    var numerator = Array.empty[Feature[_]]
    var denominator = Array.empty[Feature[_]]
    var allCovered = false

    do {
      val nextPattern = next(covered, matchPatterns)
      if (nextPattern != null) {
        val nextPatternFeatures = nextPattern.predicate
        numerator = numerator.union(nextPatternFeatures).distinct
        denominator = denominator.union(nextPatternFeatures.intersect(covered)).distinct
        covered = covered.union(nextPatternFeatures).distinct
        matchPatterns = matchPatterns.filter(_ != nextPattern)

        if (covered.length >= instance.length || matchPatterns.isEmpty)
          allCovered = true
      }
      else
        allCovered = true

    } while (!allCovered)

    val numeratorProbability = probability(numerator, trainingInstances)
    val denominatorProbability = probability(denominator, trainingInstances)

    for (i ← classFeatures.indices)
      classProbability(i) = classFeatureProbability(i) * (numeratorProbability(i).product / denominatorProbability(i).product)

    classProbability
  }

  /**
    * This method select the next pattern to be used in the calc of the probability
    * of a class.
    *
    * @param B Array of ContrastPattern that match with the instance.
    * @return The next ContrastPattern to be used.
    */
  private def next(covered: Array[Feature[_]], B: Array[ContrastPattern]): ContrastPattern = {
    // Z = {s in B && |s - covered| >= 1}
    if (B.length == 1) return B(0)
    if (B.isEmpty) return null

    val Z = calcPatternStrength(B.filter(pattern ⇒ pattern.predicate.diff(covered).length >= 1))
    if (Z.isEmpty) return null

    var orderedByStrength = Z.sortBy(pair ⇒ pair._2).reverse
    orderedByStrength = orderedByStrength.filter(pair ⇒ pair._2 equals orderedByStrength.head._2)
    if (orderedByStrength.length == 1)
      return orderedByStrength.head._1

    var patterns = orderedByStrength.unzip._1.sortBy(pattern ⇒ pattern.predicate.length)
    patterns = patterns.filter(pattern ⇒ pattern.predicate.length == patterns.head.predicate.length)
    if (patterns.length == 1)
      return patterns.head

    patterns = patterns.sortBy(pattern ⇒ pattern.predicate.diff(covered).length)
    patterns.head
  }

  /**
    * Calc the strength of a EP using the formula of thr BCEP classifier.
    * If this formula is used in another place must be moved to ContrastPattern.
    *
    * @param patterns Array of ContrastPattern to calculate the strength
    * @return
    */
  private def calcPatternStrength(patterns: Array[ContrastPattern]): Array[(ContrastPattern, Double)] = {
    patterns.map(p ⇒ (p, (p.growthRate / (p.growthRate + 1) * p.positiveClassSupport)))
  }

  /**
    * Calc and save the probability estimate using m-estimate. It receive
    * an Array of Feature and the training instances to obtain the estimate.
    *
    * @param usedFeatures     Features to calc the probability for each class feature.
    * @param trainingInstance Training instances, rows.
    * @return An array for each class feature with an array for each feature in that class.
    */
  private def probability(usedFeatures: Array[Feature[_]], trainingInstance: Array[Row]) = {
    val newFeatures = usedFeatures.diff(features)
    val oldFeatures = usedFeatures.diff(newFeatures)
    val newFeaturesClassCount = Array.ofDim[Int](classFeatures.length, newFeatures.length)
    var usedFeaturesProbability = Array.ofDim[Double](classFeatures.length, usedFeatures.length)

    if (!newFeatures.isEmpty) {
      for (instance ← trainingInstance) {
        val classIndex = classFeatures.indexWhere(_._1.isMatch(instance))

        for (i ← newFeatures.indices)
          if (newFeatures(i).isMatch(instance))
            newFeaturesClassCount(classIndex)(i) += 1
      }

      features ++= newFeatures
      val newFeaturesProbability = mEstimate(newFeaturesClassCount)
      for (i ← featuresProbability.indices)
        featuresProbability(i) ++= newFeaturesProbability(i)

      for (i ← classFeatures.indices; j ← newFeatures.indices)
        usedFeaturesProbability(i)(j) = newFeaturesProbability(i)(j)
    }

    //    var k = newFeatures.length
    //    for (feature ← oldFeatures) {
    //      val j = features.indexOf(feature)
    //      for (i ← classFeatures.indices)
    //        usedFeaturesProbability(i)(k) = featuresProbability(i)(j)
    //      k += 1
    //    }

    var k = newFeatures.length
    for (feature ← oldFeatures) {
      val j = features.indexOf(feature)
      for (i ← classFeatures.indices)
        try {
          usedFeaturesProbability(i)(k) = featuresProbability(i)(j)
        }
        catch {
          case e: ArrayIndexOutOfBoundsException ⇒ {
            e.printStackTrace()
          }
        }
      k += 1
    }

    usedFeaturesProbability
  }

  /**
    * M-estimate is used to estimate the probability of features to be used
    * later in the product of classification using NB
    *
    * @param n0 Is a small number, normally with value 5
    */
  private def mEstimate(featuresClassCount: Array[Array[Int]], n0: Int = 5) = {
    val m = Array.ofDim[Double](classFeatures.length, featuresClassCount.head.length)
    for (j ← featuresClassCount.head.indices) {
      val classCountInFeature = new Array[Int](classFeatures.length)
      for (i ← classFeatures.indices)
        classCountInFeature(i) += featuresClassCount(i)(j)

      for (i ← classCountInFeature.indices) {
        m(i)(j) += (classCountInFeature(i) +
          (n0 * (classCountInFeature.sum.toDouble / dataMiner.instanceCount.toDouble))) /
          (classFeatures(i)._2 + n0).toDouble
      }
    }
    m
  }

  /**
    * Laplace-estimate is used to estimate the probability of class features to
    * be used later in the product of classification using NB
    *
    * @param k Is a small number, normally with value 1
    */
  private def laplaceEstimate(k: Int = 1): Array[Double] = {
    val arr = new Array[Double](classFeatures.length)
    for (i ← classFeatures.indices) {
      arr(i) = (classFeatures(i)._2 + k).toDouble / (dataMiner.instanceCount + classFeatures.length * k).toDouble
    }
    arr
  }

  private def pruneEP(growthRate: Double = 100) = {
    var selectedPatterns = new ArrayBuffer[ContrastPattern]()
    val patternsByGrowthRate = patterns.filter(pattern ⇒ pattern.growthRate >= growthRate)
    for (classFeatures ← classFeatures.map(_._1))
      selectedPatterns ++= pruneEPByClass(patterns, classFeatures)
    patterns = selectedPatterns.toArray
  }

  private def pruneEPByClass(patterns: Array[ContrastPattern], classFeature: CategoricalFeature) = {
    var training = trainingInstances.filter(classFeature.isMatch)
    val selectedPatterns = new ArrayBuffer[ContrastPattern]()
    val patternsClass = patterns.filter(pattern ⇒ pattern.clazz == classFeature)
      .sortBy(_.positiveClassSupport()).reverse
    val supports = patternsClass.map(_.positiveClassSupport()).distinct

    for (support ← supports) {
      val patternsBySupport = patternsClass.filter(_.positiveClassSupport.equals(support))
        .sortBy(_.predicate.length).reverse
      for (pattern ← patternsBySupport) {
        if (training.exists(pattern.isMatch(_))) {
          training = training.filter(row ⇒ !pattern.isMatch(row))
          selectedPatterns += pattern
        }
      }
    }
    selectedPatterns
  }

  override def toString: String = "BCEP"
}