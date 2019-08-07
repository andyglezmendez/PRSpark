package models

import common.feature.CategoricalFeature
import common.miner.DatasetSchema
import common.model.Model
import common.pattern.ContrastPattern
import org.apache.spark.sql.Row

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class CAEPModel extends Model {
  private var baseScore: Array[(CategoricalFeature, Double)] = _
  private var patternsByClass: Array[(CategoricalFeature, Array[(ContrastPattern, Array[Double])])] = _
  private var trainingInstancesByClass: Array[(CategoricalFeature, Array[Row])] = _
  private var patternsWithSupport: Array[(ContrastPattern, Array[Double])] = _


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
    this.classFeatures = dataMiner.classFeatures()
    this.trainingInstancesByClass = groupInstancesByClass()
    this.patternsWithSupport = getPatternsWithSupport()
    this.baseScore = getBaseScore()
  }

  /**
    * Method to predict the class of an instance. It returns an
    * Array of Double with the votes of every class.
    *
    * @param instance An instance to be classified
    * @return Array with the votes of every class
    */
  override def predict(instance: Row): Array[Double] = {
    val scores = getScore(instance)
    val finalScores = new Array[Double](scores.length)
    for (i ← scores.indices) {
      if (baseScore(i)._2 > 0)
        finalScores(i) = scores(i) / baseScore(i)._2
      else
        finalScores(i) = 0
    }

    finalScores
  }

  /**
    * Calc patterns support in all classes
    *
    * @return
    */
  def getPatternsWithSupport(): Array[(ContrastPattern, Array[Double])] = {
    val pattWithSupport = new ArrayBuffer[(ContrastPattern, Array[Double])](patterns.length)
    val classes = dataMiner.classFeatures().map(_._1)
    val iBC = trainingInstancesByClass
    for (pattern ← patterns) {
      val supports = new Array[Double](classes.length)
      for (i ← iBC.indices) {
        val index = classes.indexWhere(cf ⇒ cf == iBC(i)._1)
        supports(index) = iBC(i)._2.count(row ⇒ pattern.predicate.forall(_.isMatch(row))) / iBC(i)._2.length.toDouble
      }
      pattWithSupport += Tuple2(pattern, supports)
    }
    pattWithSupport.toArray
  }

  /**
    * Group instances by classes
    *
    * @return
    */
  def groupInstancesByClass(): Array[(CategoricalFeature, Array[Row])] = {
    val hash = new Array[(CategoricalFeature, ArrayBuffer[Row])](dataMiner.classFeatures().length)
    val classes = dataMiner.classFeatures().map(_._1)
    for (i ← classes.indices) {
      hash(i) = (classes(i), new ArrayBuffer[Row])
    }

    for (instance ← trainingInstances) {
      val index = hash.indexWhere(pair ⇒ pair._1.isMatch(instance))
      hash(index)._2 += instance
    }

    hash.map(pair ⇒ (pair._1, pair._2.toArray))
  }

  /**
    * Calc score for all classes
    *
    * @param instance
    * @return
    */
  def getScore(instance: Row): Array[Double] = {
    val classes = classFeatures.map(_._1)
    val scores: Array[Double] = new Array(classes.length)

    for (i ← classes.indices) {
      val patternsInstance = patternsWithSupport.filter(_._1.predicate.forall(_.isMatch(instance)))

      scores(i) = patternsInstance.map(pair ⇒ {
        val pattern = pair._1
        val div = pattern.growthRate() / (pattern.growthRate() + 1)

        if (pattern.growthRate() == Double.PositiveInfinity)
          pair._2(i)
        else if (pattern.growthRate() == 0)
          0
        else
          pair._2(i) * div
      }).sum
    }

    scores
  }

  /**
    * Calc the base score. A median of scores of instances by class
    *
    * @return
    */
  def getBaseScore(): Array[(CategoricalFeature, Double)] = {
    val classes = classFeatures.map(_._1)
    val bScore = new Array[(CategoricalFeature, Double)](classes.length)

    for (i ← classes.indices) {
      val instances = trainingInstancesByClass(i)._2
      val score: Double = instances.map(row ⇒ getScore(row)(i)).sum / instances.length.toDouble
      bScore(i) = (classes(i), score)
    }

    bScore
  }

  override def toString: String = "CAEP"
}
