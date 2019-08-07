import common.miner.evaluator.CrossValidationEvaluator
import common.dataframe.dicretizer.{EntropyBasedDiscretizer, IntegerToStringDiscretizer}
import common.utils.ClassSelector
import experiment.ModelEvaluation
import miners.epm.miner._
import miners.pr_framework.miner._
import miners.pr_spark.fp_max.FPMaxMiner
import models.iCAEPModel
import org.junit.Test

class iCAEPModelEvaluation extends ModelEvaluation {

  val db = "wine.arff"
  val path = "resource\\db\\"
  val splitEvaluate = 10

  @Test
  def run(): Unit = {
    initializeSpark()
    var ds = loadDataFrame(path + db)
    val discretizer = new EntropyBasedDiscretizer
    ds = discretizer.discretize(ds)

    ds.cache()
    ds.printSchema()

    val cross = new CrossValidationEvaluator(ds, new RandomForestMiner, new iCAEPModel)
    cross.evaluate(splitEvaluate)
    println(s"Accuracy => ${cross.effectiveness()}")

  }


  //EPM
  @Test
  def test_epm_bcep(): Unit = {
    initializeSpark()
    var ds = loadDataFrame(path + db)
    val discretizer = new EntropyBasedDiscretizer
    ds = discretizer.discretize(ds)

    ds.cache()
    ds.printSchema()

    val cross = new CrossValidationEvaluator(ds, new BCEPMiner, new iCAEPModel)
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

    val cross = new CrossValidationEvaluator(ds, new DeEPSMiner, new iCAEPModel)
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

    val cross = new CrossValidationEvaluator(ds, new DGCPTreeMiner, new iCAEPModel)
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

    val cross = new CrossValidationEvaluator(ds, new IEPMiner, new iCAEPModel)
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

    val cross = new CrossValidationEvaluator(ds, new SJEPMiner, new iCAEPModel)
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

    val cross = new CrossValidationEvaluator(ds, new TopKMiner, new iCAEPModel)
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

    val cross = new CrossValidationEvaluator(ds, new TreeBasedJEPMiner, new iCAEPModel)
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

    val cross = new CrossValidationEvaluator(ds, new BaggingMiner, new iCAEPModel)
    cross.evaluate(splitEvaluate)
    println(s"Accuracy => ${cross.effectiveness()}")

  }

  @Test
  def test_prf_cepm(): Unit = {
    initializeSpark()
    var ds = loadDataFrame(path + db)
    ds = ClassSelector.selectClass(ds,"class")
//    ds = new IntegerToStringDiscretizer().discretize(ds)
//    val discretizer = new EntropyBasedDiscretizer
//    ds = discretizer.discretize(ds)

    ds.cache()
    ds.printSchema()

    val cross = new CrossValidationEvaluator(ds, new CEPMMiner, new iCAEPModel)
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

    val cross = new CrossValidationEvaluator(ds, new DeleteBestConditionMiner, new iCAEPModel)
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

    val cross = new CrossValidationEvaluator(ds, new DeleteBestConditionByLevelMiner, new iCAEPModel)
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

    val cross = new CrossValidationEvaluator(ds, new DeleteBetterFeatureMiner, new iCAEPModel)
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

    val cross = new CrossValidationEvaluator(ds, new LCMiner, new iCAEPModel)
    cross.evaluate(splitEvaluate)
    println(s"Accuracy => ${cross.effectiveness()}")

  }

  @Test
  def test_prf_random_forest(): Unit = {
    initializeSpark()
    var ds = loadDataFrame(path + db)
    ds = ClassSelector.selectClass(ds,"class")
//    val discretizer = new EntropyBasedDiscretizer
//    ds = discretizer.discretize(ds)

    ds.cache()
    ds.printSchema()

    val cross = new CrossValidationEvaluator(ds, new RandomForestMiner, new iCAEPModel)
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

    val cross = new CrossValidationEvaluator(ds, new RandomSplitMiner, new iCAEPModel)
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

    val cross = new CrossValidationEvaluator(ds, new RandomSubsetMiner, new iCAEPModel)
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

    val cross = new CrossValidationEvaluator(ds, new FPMaxMiner, new iCAEPModel)
    cross.evaluate(splitEvaluate)
    println(s"Accuracy => ${cross.effectiveness()}")

  }

}
