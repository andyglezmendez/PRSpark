package experiment.accuracy

import common.miner.evaluator.CrossValidationEvaluator
import common.model.Model
import common.utils.ClassSelector
import experiment.{ModelEvaluation, ResultSerializer}
import miners.epm.miner.DGCPTreeMiner

import scala.collection.mutable.ArrayBuffer

class AccuracyExperiment(experimentPath: String) extends ResultSerializer with ModelEvaluation {

  val dataBases = new ArrayBuffer[String]()
  val supports = new ArrayBuffer[Double]()
  var miner: DGCPTreeMiner = _
  val classifiers = new ArrayBuffer[Model]()
  val evaluators = new ArrayBuffer[CrossValidationEvaluator]()


  def run(kSplits: Int = 10) = {
    initializeSpark()
    deletePreviousResults(experimentPath, "*-accuracy.data")
    initializeSpark()
    for (db ← dataBases) {
      for (support ← supports) {
        miner = new DGCPTreeMiner
        miner.MIN_SUPPORT = support
        for (classifier ← classifiers) {
          var df = loadDataFrame(db)
          df = ClassSelector.selectClass(df, "class")
          df.cache()
          df.printSchema()
          val cross = new CrossValidationEvaluator(df, miner, classifier)
          //            cross.filterPatterns = { cp ⇒ cp.positiveClassSupport() >= support }
          cross.evaluate(kSplits)
          save(dbName(db), support, cross)
        }
      }
    }
  }

  def save(db: String, support: Double, cross: CrossValidationEvaluator) = {
//    val data = new AccuracyDataResult(db, support, cross.effectiveness())
//    val path = s"$experimentPath\\${cross.classifier.toString}-accuracy.data"
//    saveData(path, data)
  }

}
