package common.filter.chosen_few_measures

import common.utils.PatternToBinaryFeature

trait ChosenFewMeasure {

  def getMeasure(set: Array[Int], newSet: Array[Int], matrixMaker: PatternToBinaryFeature): Double

}
