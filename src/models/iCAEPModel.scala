package models

import common.feature.{CategoricalFeature, Feature}
import common.miner.DatasetSchema
import common.model.Model
import common.pattern.ContrastPattern
import org.apache.spark.sql.Row

import scala.collection.mutable.ArrayBuffer

class iCAEPModel extends Model {

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
  }

  /**
    * Method to predict the class of an instance. It returns an
    * Array of Float with the votes of every class.
    *
    * @param instance An instance to be classified
    * @return Array with the votes of every class
    */
  override def predict(instance: Row): Array[Double] = {
    val prediction = new Array[Double](classFeatures.length)

    for (i ← classFeatures.indices) {
      val selectedPatterns = selectEP(instance, classFeatures(i)._1)
      val length = selectedPatterns.map(pattern ⇒ estimateProbability(pattern))
        .map(prob ⇒ {math.log(prob) / math.log(2.0)})
        .sum

      prediction(i) = length * -1
    }

    prediction
  }

  /**
    * Estimate the probability of each pattern to belong to his positive
    * class.
    *
    * NPC pp
    *
    *
    * @param pattern Pattern to compute the probability
    * @return The estimate probability
    */
  def estimateProbability(pattern: ContrastPattern): Double = {
    val numerator = pattern.pp.toDouble + 2 * (pattern.pp.toDouble + pattern.pn.toDouble) / dataMiner.instanceCount
    val classCount = classFeatures.find(pair ⇒ pair._1 == pattern.clazz).orNull._2
    val denominator = 2 + classCount
    numerator / denominator
  }

  private def selectEP(instance: Row, clazz: CategoricalFeature): Array[ContrastPattern] = {
    val selectedPatterns = new ArrayBuffer[ContrastPattern]()
    val classPatterns = patterns.filter(p ⇒ p.clazz == clazz).sortBy(p ⇒ p.predicate.length).reverse.filter(_.isMatch(instance))
    if (!classPatterns.isEmpty) {
      var size = classPatterns.head.predicate.length
      var covered = new ArrayBuffer[Feature[_]]()
      while (size > 0) {
        val patternsLength = classPatterns.filter(_.predicate.length == size).sortBy(p ⇒ p.growthRate()).reverse
        for (selectedPattern ← patternsLength) {
          if (selectedPattern.predicate.intersect(covered).isEmpty) {
            selectedPatterns += selectedPattern
            covered = covered.union(selectedPattern.predicate).distinct
          }
          if (covered.length >= instance.length)
            return selectedPatterns.toArray
        }
        size -= 1
        if (covered.length >= instance.length)
          return selectedPatterns.toArray
      }
    }
    return selectedPatterns.toArray
  }

  override def toString: String = "iCAEP"

}
