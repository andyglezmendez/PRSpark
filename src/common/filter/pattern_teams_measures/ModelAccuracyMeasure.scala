package common.filter.pattern_teams_measures

import common.miner.evaluator.CrossValidationEvaluator
import common.model.Model
import common.pattern.ContrastPattern
import common.utils.{BitArray, PatternToBinaryFeature}
import miners.pr_spark.MockMiner
import models.DTMModel
import org.apache.spark.sql.DataFrame

class ModelAccuracyMeasure {

  def getMeasure(patterns: Array[ContrastPattern], dataFrame: DataFrame, classifier: Model = new DTMModel): Double = {
    val evaluator = new CrossValidationEvaluator(dataFrame, new MockMiner(patterns), classifier)
    evaluator.effectiveness()
  }

  def getMeasure(patterns: Array[Int], c: BitArray, matrixMaker: PatternToBinaryFeature): Double = {
    val arr = matrixMaker.getBitArrayFromPatterns(patterns)
    var codeInstances = Array.emptyIntArray

    for (i ← 0 until matrixMaker.rowsCount) {
      if (arr.forall(pattern ⇒ pattern(i) == c(i)))
        codeInstances = codeInstances :+ i
    }
    1
  }

  def dtmClassifier(label: Int, cInstances: Array[Int], matrixMaker: PatternToBinaryFeature): Double = {
    val patterns = matrixMaker.getPatternsFromClass(label).map(matrixMaker.matrix(_))
    var instances = new BitArray(matrixMaker.rowsCount)
    instances.setAllValues(false)
    patterns.foreach(p ⇒ instances = p or instances)
    val instanceFromPatterns = (0 until matrixMaker.rowsCount).toArray.filter(instances(_))

    if (cInstances.length > 0)
      (cInstances intersect instanceFromPatterns).length.toDouble / cInstances.length.toDouble
    else
      instanceFromPatterns.length.toDouble / matrixMaker.rowsCount.toDouble
  }

}
