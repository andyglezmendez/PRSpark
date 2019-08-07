package experiment.jep

import common.dataframe.dicretizer.{EntropyBasedDiscretizer, IntegerToStringDiscretizer}
import common.dataframe.noiser.RandomNoise
import common.feature.CategoricalFeature
import common.miner.{DatasetSchema, SparkMiner}
import common.model.Model
import common.pattern.ContrastPattern
import common.utils.ClassSelector
import experiment.{ModelEvaluation, ResultSerializer}
import miners.epm.miner.SJEPMiner
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ArrayBuffer

class JEPExperiment(experimentPath: String) extends ResultSerializer with ModelEvaluation {

  val dataBases = new ArrayBuffer[String]()
  val noises = new ArrayBuffer[Double]()
  var miner: SparkMiner = _
  val classifiers = new ArrayBuffer[Model]()

  var classFeatures: Array[(CategoricalFeature, Int)] = _
  var good: Array[Int] = _
  var total: Array[Int] = _
  var patterns: Array[Array[ContrastPattern]] = _

  def run(kSplits: Int = 10) = {
    deletePreviousResults(experimentPath, "*-jep.data")
    initializeSpark()
    for (db ← dataBases) {
      var df = loadDataFrame(db)
      df = ClassSelector.selectClass(df, "class")
      df.cache()
      df = new EntropyBasedDiscretizer().discretize(df)
      df.cache()
      df = new IntegerToStringDiscretizer().discretize(df)
      df.cache()

      val splitSize = 1 / kSplits.toDouble
      val splits = new Array[Double](kSplits).map(zero ⇒ splitSize)
      val acumulatedAccuracy = new Array[Double](kSplits)

      val dfSplits: Array[DataFrame] = df.randomSplit(splits)
      val trainingInstances: Array[DataFrame] = new Array[DataFrame](kSplits)

      dfSplits.foreach(_.cache())

      for (i ← splits.indices) {
        var training: DataFrame = null
        for (j ← splits.indices)
          if (j != i) {
            if (training == null) training = dfSplits(j)
            else training union dfSplits(j)
          }
        trainingInstances(i) = training
        trainingInstances(i).cache()
      }

      classFeatures = new DatasetSchema(df.collect()).classFeatures()
      good = new Array[Int](classFeatures.length)
      total = new Array[Int](classFeatures.length)

      for (noise ← noises) {
        patterns = new Array[Array[ContrastPattern]](kSplits)
        val trainingInstancesWithNoise = new Array[DataFrame](kSplits)
        val dataMinersSchemas = new Array[DatasetSchema](kSplits)

        for (i ← splits.indices) {
          trainingInstancesWithNoise(i) = RandomNoise.introduceNoise(trainingInstances(i), noise)
          trainingInstancesWithNoise(i).cache()
          patterns(i) = miner.mine(trainingInstancesWithNoise(i).collect())
          patterns(i) = patterns(i).filter(p ⇒ p.growthRate() == Double.PositiveInfinity)
          dataMinersSchemas(i) = miner.dataMiner
          println(s"SPLIT MINE OF ${i}")
        }

        for (classifier ← classifiers) {
          for (i ← splits.indices) {
            val model = classifier.getClass.newInstance()
            model.initialize(patterns(i), trainingInstancesWithNoise(i).collect(), dataMinersSchemas(i))
            calcAcummulatedAccuracy(model, dfSplits(i).collect())
            printStatistics()
          }
          save(dbName(db), noise, classifier)
        }
      }
    }
  }

  def save(db: String, noise: Double, classifier: Model) = {
//    val patternLength = patterns.map(arr ⇒ arr.map(_.predicate.length).sum / arr.length).sum / patterns.length
//
//    val data = new JEPDataResult(db, noise, effectiveness(), patternLength)
//    val path = s"$experimentPath\\${classifier.toString}-jep.data"
//    saveData(path, data)
  }

  def calcAcummulatedAccuracy(classifier: Model, test: Array[Row]) = {
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
