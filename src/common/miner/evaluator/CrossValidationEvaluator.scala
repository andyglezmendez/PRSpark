package common.miner.evaluator

import common.dataframe.dicretizer.{EntropyBasedDiscretizer, IntegerToStringDiscretizer}
import common.dataframe.noiser.RandomNoise
import common.feature.CategoricalFeature
import common.miner.{DatasetSchema, SparkMiner}
import common.model.Model
import common.pattern.ContrastPattern
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


class CrossValidationEvaluator(val dataFrame: DataFrame, val miner: SparkMiner, var classifier: Model) {

  private var good: Array[Int] = _
  private var total: Array[Int] = _
  private var classFeatures: Array[(CategoricalFeature, Int)] = _
  var print: Boolean = true
  var discretize = true
  var withNoise = false
  var noise: Double = 0.0

  var rowSplits: Array[Array[Row]] = _
  var patterns: Array[Array[ContrastPattern]] = _
  var filterPatterns: (Array[ContrastPattern], Array[Row]) ⇒ Array[ContrastPattern] = { (cps: Array[ContrastPattern], _) ⇒ cps }


  def evaluate(splitsCount: Int = 10) = {
    val splitSize = 1 / splitsCount.toDouble
    val splits = new Array[Double](splitsCount).map(zero ⇒ splitSize)
    val acumulatedAccuracy = new Array[Double](splitsCount)

    var dfSplits: Array[DataFrame] = null
    if (discretize)
      dfSplits = discretized().randomSplit(splits)
    else
      dfSplits = dataFrame.randomSplit(splits)

    classFeatures = new DatasetSchema(dataFrame.collect()).classFeatures()
    good = new Array[Int](classFeatures.length)
    total = new Array[Int](classFeatures.length)
    patterns = new Array[Array[ContrastPattern]](splitsCount)


    for (i ← splits.indices) {
      val test = dfSplits(i).collect()
//      val test = rowSplits(i)
      var training: DataFrame = null
//      var training = Array.empty[Row]
      for (j ← splits.indices)
        if (j != i) {
          if(training == null) training = dfSplits(j)
          else training union dfSplits(j)
        }
//        if (j != i) training ++= rowSplits(j)

      if(withNoise) training = RandomNoise.introduceNoise(training, noise)
      val trainingRows = training.collect()

      patterns(i) = miner.mine(trainingRows)
      patterns(i) = filterPatterns(patterns(i), test)

      classifier = classifier.getClass.newInstance()
      classifier.initialize(patterns(i), trainingRows, miner.dataMiner)
      calcAcummulatedAccuracy(test)
      if (print) printStatistics()
    }

  }

  /**
    * Discretize with EntropyBasedDiscretizer and convert Integer values to String
    *
    * @return Discretized DataFrame
    */
  private def discretized(): DataFrame = {
    var df = new EntropyBasedDiscretizer().discretize(dataFrame)
    df = new IntegerToStringDiscretizer().discretize(df)
    df
  }

  private def getDataFrame(rows: Array[Row]): DataFrame = {
    val session = SparkSession.builder().getOrCreate()
    session.createDataFrame(session.sparkContext.parallelize(rows), rows(0).schema)
  }

  def calcAcummulatedAccuracy(test: Array[Row]) = {
    for (instance ← test) {
      val predict = classifier.predict(instance)
      val clazz = classifier.getPredictionClass(predict)
      if (clazz.isMatch(instance)) {
        val goodClass = classFeatures.indexWhere(_._1 == clazz)
        good(goodClass) += 1
        total(goodClass) += 1
      }
      else {
        val badClass = classFeatures.indexWhere(_._1.isMatch(instance))
        total(badClass) += 1
      }
    }
  }

  def printStatistics() = {
    for (i ← classFeatures.indices) {
      println(s"${classFeatures(i)._1}(${classFeatures(i)._2}) => ${good(i)} / ${total(i) - good(i)} / ${total(i)}")
    }
    println(s"Good ${good.sum} from ${total.sum}")
    println(s"The accuracy is: ${effectiveness()}%")
  }

  def effectiveness(): Double = good.sum.toDouble / total.sum * 100

}
