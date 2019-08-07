package common.filter.chosen_few_measures

import common.miner.SparkMiner
import common.miner.evaluator.CrossValidationEvaluator
import common.model.Model
import common.utils.PatternToBinaryFeature
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

class PatternsInference(dataFrame: DataFrame, miner: SparkMiner, classifier: Model) extends ChosenFewMeasure {

  var evaluator = new CrossValidationEvaluator(dataFrame, miner, classifier)

  def a(matrixMaker: PatternToBinaryFeature, newSet: Array[Int]): Unit = {
    val rows = matrixMaker.instancesToBinaryRowsFromPatterns(newSet)
    val patterns = miner.mine(rows)

    val spark = SparkSession.builder().getOrCreate()

    val rdd = spark.sparkContext.parallelize(rows)
    val df = spark.createDataFrame(rdd, rdd.first().schema)
    df.cache()

    val evaluator = new CrossValidationEvaluator(df, miner, classifier)


  }

  override def getMeasure(set: Array[Int], newSet: Array[Int], matrixMaker: PatternToBinaryFeature): Double = {

    1 - evaluator.effectiveness() / matrixMaker.rowsCount
  }
}
