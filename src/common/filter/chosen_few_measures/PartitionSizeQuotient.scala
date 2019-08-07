package common.filter.chosen_few_measures

import common.filter.ChosenFewSetConstraint
import common.utils.PatternToBinaryFeature

class PartitionSizeQuotient extends ChosenFewMeasure {
  override def getMeasure(patternSet: Array[Int], newPatternSet: Array[Int], matrixMaker: PatternToBinaryFeature): Double = {
    1 - (ChosenFewSetConstraint.equivalenceRelation(patternSet, matrixMaker.matrix).toDouble /
      ChosenFewSetConstraint.equivalenceRelation(newPatternSet, matrixMaker.matrix))
  }
}
