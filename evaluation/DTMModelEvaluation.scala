import common.dataframe.dicretizer.EntropyBasedDiscretizer
import common.filter.{ChosenFewSetConstraint, JEPFilter}
import common.filter.chosen_few_measures.AgglomerativeClustering
import common.miner.evaluator.CrossValidationEvaluator
import common.pattern.ContrastPattern
import common.utils.ClassSelector
import experiment.ModelEvaluation
import miners.epm.miner.{BCEPMiner, SJEPMiner}
import miners.pr_framework.miner.{LCMiner, RandomForestMiner}
import models.{BCEPModel, DTMModel}
import org.apache.spark.sql.Row
import org.junit.Test

class DTMModelEvaluation extends ModelEvaluation {

  val db = "iris.arff"
  val path = "test_resources\\arff\\"
  val splitEvaluate = 2

  //PRS
  @Test
  def test_prs_dtm(): Unit = {
    initializeSpark()
    var ds = loadDataFrame(path + db)
    val discretizer = new EntropyBasedDiscretizer
    ds = discretizer.discretize(ds)

    ds.cache()
    ds.printSchema()

    val cross = new CrossValidationEvaluator(ds, new RandomForestMiner, new DTMModel)
    cross.filterPatterns = { (cp: Array[ContrastPattern], rows: Array[Row]) ⇒
      val constraint = new ChosenFewSetConstraint(rows)
      constraint.measure = new AgglomerativeClustering
      constraint.getPatternSet(cp, 0.02)
    }

    cross.evaluate(splitEvaluate)
    println(s"Accuracy => ${cross.effectiveness()}")

  }


  @Test
  def test_jep(): Unit = {
    initializeSpark()
    var ds = loadDataFrame(path + db)
    ds.printSchema()
    val df = ClassSelector.selectClass(ds,ds.columns(0))
    df.printSchema()
//    val discretizer = new EntropyBasedDiscretizer
//    ds = discretizer.discretize(ds)
//
//    ds.cache()
//    ds.printSchema()
//
//
//    val miner = new LCMiner()
//    miner.MIN_SUPPORT = 0.05
//    var patterns = miner.mine(ds.collect())
//    val filter = new JEPFilter()
//    val jeps = filter.filter(patterns)
//    val jeps = patterns
//
//    println(s"JEP COUNT ${jeps.length}")
//    jeps.foreach(p ⇒ println(p))
  }

}
