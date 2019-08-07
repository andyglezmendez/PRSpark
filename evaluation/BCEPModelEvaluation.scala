import common.miner.evaluator.CrossValidationEvaluator
import common.dataframe.dicretizer.EntropyBasedDiscretizer
import experiment.ModelEvaluation
import miners.epm.miner._
import miners.pr_framework.miner._
import miners.pr_spark.fp_max.FPMaxMiner
import models.BCEPModel
import org.junit.Test

class BCEPModelEvaluation extends ModelEvaluation {

  val db = "iris.arff"
  val path = "test_resources\\arff\\"
  val splitEvaluate = 2

  //EPM
  @Test
  def test_epm_bcep(): Unit = {
    initializeSpark()
    var ds = loadDataFrame(path + db)
    val discretizer = new EntropyBasedDiscretizer
    ds = discretizer.discretize(ds)

    ds.cache()
    ds.printSchema()

    val cross = new CrossValidationEvaluator(ds, new BCEPMiner, new BCEPModel)
    cross.evaluate(splitEvaluate)
    println(s"Accuracy => ${cross.effectiveness()}")

  }

  @Test
  def test_epm_deeps(): Unit = {
    initializeSpark()
    var ds = loadDataFrame(path + db)
    val discretizer = new EntropyBasedDiscretizer
    ds = discretizer.discretize(ds)

    ds.cache()
    ds.printSchema()

    val cross = new CrossValidationEvaluator(ds, new DeEPSMiner, new BCEPModel)
    cross.evaluate(splitEvaluate)
    println(s"Accuracy => ${cross.effectiveness()}")

  }

  @Test
  def test_epm_dgcp(): Unit = {
    initializeSpark()
    var ds = loadDataFrame(path + db)
    val discretizer = new EntropyBasedDiscretizer
    ds = discretizer.discretize(ds)

    ds.cache()
    ds.printSchema()

    val cross = new CrossValidationEvaluator(ds, new DGCPTreeMiner, new BCEPModel)
    cross.evaluate(splitEvaluate)
    println(s"Accuracy => ${cross.effectiveness()}")

  }

  @Test
  def test_epm_iep(): Unit = {
    initializeSpark()
    var ds = loadDataFrame(path + db)
    val discretizer = new EntropyBasedDiscretizer
    ds = discretizer.discretize(ds)

    ds.cache()
    ds.printSchema()

    val cross = new CrossValidationEvaluator(ds, new IEPMiner, new BCEPModel)
    cross.evaluate(splitEvaluate)
    println(s"Accuracy => ${cross.effectiveness()}")

  }

  @Test
  def test_epm_sjep(): Unit = {
    initializeSpark()
    var ds = loadDataFrame(path + db)
    val discretizer = new EntropyBasedDiscretizer
    ds = discretizer.discretize(ds)

    ds.cache()
    ds.printSchema()

    val cross = new CrossValidationEvaluator(ds, new SJEPMiner, new BCEPModel)
    cross.evaluate(splitEvaluate)
    println(s"Accuracy => ${cross.effectiveness()}")

  }

  @Test
  def test_epm_topk(): Unit = {
    initializeSpark()
    var ds = loadDataFrame(path + db)
    val discretizer = new EntropyBasedDiscretizer
    ds = discretizer.discretize(ds)

    ds.cache()
    ds.printSchema()

    val cross = new CrossValidationEvaluator(ds, new TopKMiner, new BCEPModel)
    cross.evaluate(splitEvaluate)
    println(s"Accuracy => ${cross.effectiveness()}")

  }

  @Test
  def test_epm_tree_based_jep(): Unit = {
    initializeSpark()
    var ds = loadDataFrame(path + db)
    val discretizer = new EntropyBasedDiscretizer
    ds = discretizer.discretize(ds)

    ds.cache()
    ds.printSchema()

    val cross = new CrossValidationEvaluator(ds, new TreeBasedJEPMiner, new BCEPModel)
    cross.evaluate(splitEvaluate)
    println(s"Accuracy => ${cross.effectiveness()}")

  }

  //PRFramework

  @Test
  def test_prf_bagging(): Unit = {
    initializeSpark()
    var ds = loadDataFrame(path + db)
    val discretizer = new EntropyBasedDiscretizer
    ds = discretizer.discretize(ds)

    ds.cache()
    ds.printSchema()

    val cross = new CrossValidationEvaluator(ds, new BaggingMiner, new BCEPModel)
    cross.evaluate(splitEvaluate)
    println(s"Accuracy => ${cross.effectiveness()}")

  }

  @Test
  def test_prf_cepm(): Unit = {
    initializeSpark()
    var ds = loadDataFrame(path + db)
    val discretizer = new EntropyBasedDiscretizer
    ds = discretizer.discretize(ds)

    ds.cache()
    ds.printSchema()

    val cross = new CrossValidationEvaluator(ds, new CEPMMiner, new BCEPModel)
    cross.evaluate(splitEvaluate)
    println(s"Accuracy => ${cross.effectiveness()}")

  }

  @Test
  def test_prf_delete_best_condition(): Unit = {
    initializeSpark()
    var ds = loadDataFrame(path + db)
    val discretizer = new EntropyBasedDiscretizer
    ds = discretizer.discretize(ds)

    ds.cache()
    ds.printSchema()

    val cross = new CrossValidationEvaluator(ds, new DeleteBestConditionMiner, new BCEPModel)
    cross.evaluate(splitEvaluate)
    println(s"Accuracy => ${cross.effectiveness()}")

  }

  @Test
  def test_prf_delete_best_condition_level(): Unit = {
    initializeSpark()
    var ds = loadDataFrame(path + db)
    val discretizer = new EntropyBasedDiscretizer
    ds = discretizer.discretize(ds)

    ds.cache()
    ds.printSchema()

    val cross = new CrossValidationEvaluator(ds, new DeleteBestConditionByLevelMiner, new BCEPModel)
    cross.evaluate(splitEvaluate)
    println(s"Accuracy => ${cross.effectiveness()}")

  }

  @Test
  def test_prf_delete_better_feature(): Unit = {
    initializeSpark()
    var ds = loadDataFrame(path + db)
    val discretizer = new EntropyBasedDiscretizer
    ds = discretizer.discretize(ds)

    ds.cache()
    ds.printSchema()

    val cross = new CrossValidationEvaluator(ds, new DeleteBetterFeatureMiner, new BCEPModel)
    cross.evaluate(splitEvaluate)
    println(s"Accuracy => ${cross.effectiveness()}")

  }

  @Test
  def test_prf_lc(): Unit = {
    initializeSpark()
    var ds = loadDataFrame(path + db)
    val discretizer = new EntropyBasedDiscretizer
    ds = discretizer.discretize(ds)

    ds.cache()
    ds.printSchema()

    val cross = new CrossValidationEvaluator(ds, new LCMiner, new BCEPModel)
    cross.evaluate(splitEvaluate)
    println(s"Accuracy => ${cross.effectiveness()}")

  }

  @Test
  def test_prf_random_forest(): Unit = {
    initializeSpark()
    var ds = loadDataFrame(path + db)
    val discretizer = new EntropyBasedDiscretizer
//    ds = discretizer.discretize(ds)

    ds.cache()
    ds.printSchema()

    val cross = new CrossValidationEvaluator(ds, new RandomForestMiner, new BCEPModel)
    cross.evaluate(splitEvaluate)
    println(s"Accuracy => ${cross.effectiveness()}")

  }

  @Test
  def test_prf_random_split(): Unit = {
    initializeSpark()
    var ds = loadDataFrame(path + db)
    val discretizer = new EntropyBasedDiscretizer
    ds = discretizer.discretize(ds)

    ds.cache()
    ds.printSchema()

    val cross = new CrossValidationEvaluator(ds, new RandomSplitMiner, new BCEPModel)
    cross.evaluate(splitEvaluate)
    println(s"Accuracy => ${cross.effectiveness()}")

  }

  @Test
  def test_prf_random_subset(): Unit = {
    initializeSpark()
    var ds = loadDataFrame(path + db)
    val discretizer = new EntropyBasedDiscretizer
    ds = discretizer.discretize(ds)

    ds.cache()
    ds.printSchema()

    val cross = new CrossValidationEvaluator(ds, new RandomSubsetMiner, new BCEPModel)
    cross.evaluate(splitEvaluate)
    println(s"Accuracy => ${cross.effectiveness()}")

  }

  //PRSpark
  @Test
  def test_prs_fpmax(): Unit = {
    initializeSpark()
    var ds = loadDataFrame(path + db)
    val discretizer = new EntropyBasedDiscretizer
    ds = discretizer.discretize(ds)

    ds.cache()
    ds.printSchema()

    val cross = new CrossValidationEvaluator(ds, new FPMaxMiner, new BCEPModel)
    cross.evaluate(splitEvaluate)
    println(s"Accuracy => ${cross.effectiveness()}")

  }

}
