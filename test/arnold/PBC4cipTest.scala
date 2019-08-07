package arnold

import PRFramework.Core.SupervisedClassifiers.DecisionTrees.DistributionEvaluators.Hellinger
import classifiers.unbalnce_classifiers.pbc4.{InstantiatePBC4cip, PBC4cip}
import common.feature.{CategoricalFeature, EqualThan, Feature, IntegerFeature}
import common.pattern.ContrastPattern
import miners.pr_framework.miner.RandomForestMiner
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.junit.{Before, Test}

class PBC4cipTest {

  var ds: Dataset[Row] = null

  @Test
  def validationTestWithGlassDataBase() = {
    validationTest("glass1.csv")
  }

  @Test
  def validationTestWithEcoliDataBase() = {
    validationTest("ecoli-0_vs_1.csv")
  }

  @Test
  def validationTestWithIris0DataBase() = {
    validationTest("iris0.csv")
  }

  @Test
  def validationTestWithPimaDataBase() = {
    validationTest("pima.csv")
  }

  /*
 @Test
 def validationTestWithAdultDataBase() = {
   validationTest("adult.csv")
 }

   @Test
 def validationTestWithAbalone19DataBase() = {
   validationTest("abalone19.csv")
 }
*/
  @Test
  def validationTestWitWineDataBase() = {
    validationTest("wine.csv")
  }


  @Test
  def validationTestWithWisconsinDataBase() = {
    validationTest("wisconsin.csv")
  }

  private def validationTest(databaseName: String) = {
    val intializeSparkEnviromentResult: (PBC4cip, Array[Row]) = intializeSparkEnviroment(databaseName)
    var classifier: PBC4cip = intializeSparkEnviromentResult._1
    val rows: Array[Row] = intializeSparkEnviromentResult._2

    var countTrue = 0d
    for (i <- rows) {
      var r = classifier.classify(i)
      if (r.clazzFeature.isMatch(i)) {
        countTrue += 1
      }
    }

    if (databaseName.equals("glass1.csv"))
      specificThings(classifier, rows, "positive", "negative")
    else if (databaseName.equals("iris0.csv"))
      specificThings(classifier, rows, "positive", "negative")
    else if (databaseName.equals("pima.csv"))
      specificThings(classifier, rows, "positive", "negative")
    else if (databaseName.equals("ecoli-0_vs_1.csv"))
      specificThings(classifier, rows, "negative", "positive")
    else if (databaseName.equals("wisconsin.csv"))
      specificThings(classifier, rows, "positive", "negative")
    else if (databaseName.equals("wisconsin.csv"))
      specificThings(classifier, rows, "positive", "negative")
    else if (databaseName.equals("wine.csv"))
      specificThings(classifier, rows, "3", "<=50K")
    else if (databaseName.equals("adult.csv"))
      specificThings(classifier, rows, ">50K", "<=50K")


    println("Precision de la clasificación: " + (countTrue / rows.length).toString + " %")
    assert(countTrue / rows.length > 0.5)
  }

  private def specificThings(classifier: PBC4cip, rows: Array[Row], minority: String, mayority: String) = {
    var totalOfMinorityInstances = 0
    for (row <- rows) {
      if (row.get(row.schema.fields.length - 1).equals(minority)) {
        totalOfMinorityInstances += 1
      }
    }

    var countInstancesMinority = 0
    for (i <- rows) {
      var r = classifier.classify(i)
      if (r.clazzFeature.isMatch(i) && i.get(i.schema.fields.length - 1).equals(minority)) {
        countInstancesMinority += 1
      }
    }

    println("Instancias minoritarias clasificadas : " + countInstancesMinority + " \\ " + totalOfMinorityInstances)

    var totalOfMayorityInstances = 0
    for (row <- rows) {
      if (row.get(row.schema.fields.length - 1).equals(mayority)) {
        totalOfMayorityInstances += 1
      }
    }

    var countInstancesMayority = 0
    for (i <- rows) {
      var r = classifier.classify(i)
      if (r.clazzFeature.isMatch(i) && i.get(i.schema.fields.length - 1).equals(mayority)) {
        countInstancesMayority += 1
      }
    }

    println("Instancias mayoritarias clasificadas : " + countInstancesMayority + " \\ " + totalOfMayorityInstances)
  }

  private def intializeSparkEnviroment(databaseName: String) = {
    val conf = new SparkConf().setMaster("local")
      .setAppName("My App")
      .set("spark.sql.warehouse.dir", "..\\")

    val ss = SparkSession.builder().config(conf).getOrCreate()

    var ds = ss.sqlContext.read.format("csv").option("header", "true").load("resource\\" + databaseName)
    //ds.cache()

    val Array(training, test) = ds.randomSplit(Array(0.7, 0.3))
    training.cache()
    test.cache()

    //Minador afectado con la metrica de helinger
    val miner = new RandomForestMiner();
    miner.DISTRIBUTION_EVALUATOR = new Hellinger
    val patterns = miner.mine(training.collect())
    //Minador afectado con la metrica de helinger

    var classifier = InstantiatePBC4cip.buildClassifierWithContrastPatterns(patterns);
    classifier.train()

    val rows = test.collect()
    (classifier, rows)
  }

  private def doubleComparison(x: Double, y: Double, precision: Double): Boolean = {
    if ((x - y).abs < precision)
      return true
    return false
  }
}
