package classifiers.imbalance_classifiers

import java.util.Calendar

import classifiers.unbalnce_classifiers.pbc4.{InstantiatePBC4cip, PBC4cip}
import common.feature.{CategoricalFeature, EqualThan, Feature, IntegerFeature}
import common.pattern.ContrastPattern
import miners.pr_framework.miner.RandomForestMiner
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.junit.{Before, Test}

class PBC4cipTest {

  var contrastPatternCollection: Array[ContrastPattern] = new Array[ContrastPattern](10)

  @Before
  def initializePatterns(): Unit = {

    //Target Class
    var higherClass = new CategoricalFeature("Clase", 7, "Mayoritaria", false)
    var lowerClass = new CategoricalFeature("Clase", 7, "Minoritaria", false)

    var cp01 = buildContrastPattern("F4", 1, 3, "F5", 1, 4, higherClass)
    var cp02 = buildContrastPattern("F1", 0, 0, "F2", 0, 1, higherClass)
    var cp03 = buildContrastPattern("F1", 0, 0, "F4", 1, 3, higherClass)
    var cp04 = buildContrastPattern("F1", 0, 0, "F6", 1, 5, higherClass)
    var cp05 = buildContrastPattern("F2", 0, 1, "F4", 1, 3, higherClass)
    var cp06 = buildContrastPattern("F1", 0, 0, "F3", 0, 2, higherClass)
    var cp07 = buildContrastPattern("F6", 1, 5, "F4", 1, 3, higherClass)
    var cp08 = buildContrastPattern("F2", 0, 1, "F3", 0, 2, higherClass)
    var cp09 = buildContrastPattern("F1", 1, 0, "F2", 1, 1, lowerClass)
    var cp10 = buildContrastPattern("F2", 1, 1, "F3", 1, 2, lowerClass)

    //Añadiendo el soporte a los patrones
    //Soporte = pp/(np + pp)
    changeContingencyTable(cp01, 7, 2)
    changeContingencyTable(cp02, 6, 3)
    changeContingencyTable(cp03, 6, 3)
    changeContingencyTable(cp04, 6, 3)
    changeContingencyTable(cp05, 6, 3)
    changeContingencyTable(cp06, 4, 5)
    changeContingencyTable(cp07, 5, 4)
    changeContingencyTable(cp08, 3, 6)
    changeContingencyTable(cp09, 1, 2)
    changeContingencyTable(cp10, 1, 2)

    //Inicializando el arreglo de patrones de contraste
    contrastPatternCollection(0) = cp01
    contrastPatternCollection(1) = cp02
    contrastPatternCollection(2) = cp03
    contrastPatternCollection(3) = cp04
    contrastPatternCollection(4) = cp05
    contrastPatternCollection(5) = cp06
    contrastPatternCollection(6) = cp07
    contrastPatternCollection(7) = cp08
    contrastPatternCollection(8) = cp09
    contrastPatternCollection(9) = cp10

  }


  private def buildContrastPattern(feature1: String, value1: Double, columnIndex1: Integer,
                                   feature2: String, value2: Double, columnIndex2: Integer,
                                   targetClass: Feature[String]): ContrastPattern = {

    var f1 = new IntegerFeature(feature1, columnIndex1, value1, false) with EqualThan
    var f2 = new IntegerFeature(feature2, columnIndex2, value2, false) with EqualThan

    var predicate = new Array[Feature[_]](2)
    predicate.update(0, f1)
    predicate.update(1, f2)

    return new ContrastPattern(predicate, targetClass)
  }

  private def changeContingencyTable(contrastPattern: ContrastPattern, pp: Double, np: Double): Unit = {
    contrastPattern.pp = pp.toInt
    contrastPattern.np = np.toInt
  }

  var ds: Dataset[Row] = null

  @Test
  def initializeClassifierWithContrastPatterns(): Unit = {
    val classifier = InstantiatePBC4cip.buildClassifierWithContrastPatterns(contrastPatternCollection);
    assert(classifier != null)
  }

  @Test
  def checkingPatternSupports(): Unit = {
    val classifier = InstantiatePBC4cip.buildClassifierWithContrastPatterns(contrastPatternCollection);
    val supports: Array[Double] = classifier.getPatternSupports()

    assert(doubleComparison(supports(0), 0.77, 0.1))
    assert(doubleComparison(supports(1), 0.66, 0.1))
    assert(doubleComparison(supports(2), 0.66, 0.1))
    assert(doubleComparison(supports(3), 0.66, 0.1))
    assert(doubleComparison(supports(4), 0.66, 0.1))
    assert(doubleComparison(supports(5), 0.44, 0.1))
    assert(doubleComparison(supports(6), 0.56, 0.1))
    assert(doubleComparison(supports(7), 0.33, 0.1))
    assert(doubleComparison(supports(8), 0.33, 0.1))
    assert(doubleComparison(supports(9), 0.33, 0.1))
  }

  @Test
  def trainClassifier: Unit = {
    val classifier = InstantiatePBC4cip.buildClassifierWithContrastPatterns(contrastPatternCollection);
    classifier.train()
    val weightPerClass = classifier.getWeightPerClass


    assert(doubleComparison(0.02, weightPerClass(0), 0.1))
    assert(doubleComparison(1.17, weightPerClass(1), 0.1))
  }

  @Test
  def classify: Unit = {

    import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
    import org.apache.spark.sql.types._
    val values: Array[Any] = Array(0, 1, 1, 1, 1, 1)
    val schema = StructType(
      StructField("F1", IntegerType, false) ::
        StructField("F2", IntegerType, false) ::
        StructField("F3", IntegerType, false) ::
        StructField("F4", IntegerType, false) ::
        StructField("F5", IntegerType, false) ::
        StructField("F6", IntegerType, false) :: Nil
    )
    val instance = new GenericRowWithSchema(values, schema)

    val classifier = InstantiatePBC4cip.buildClassifierWithContrastPatterns(contrastPatternCollection);

    val timeStart = Calendar.getInstance().getTimeInMillis;

    classifier.train()
    val result = classifier.classify(instance)

    val timeEnd = Calendar.getInstance().getTimeInMillis;

    println(timeEnd - timeStart)
  }

  @Test
  def validationTestWithGlassDataBase() = {
    validationTest("glass1.csv", 0.4, "positive", "negative")
  }

  @Test
  def validationTestWithPimaDataBase() = {
    validationTest("pima.csv", 0.4, "positive", "negative")
  }

  @Test
  def validationTestWithEcoliDataBase() = {
    validationTest("ecoli-0_vs_1.csv", 0.4, "negative", "positive")
  }

  @Test
  def validationTestWithIris0DataBase() = {
    validationTest("iris0.csv", 0.4, "positive", "negative")
  }

  @Test
  def validationTestWithWisconsinDataBase() = {
    validationTest("wisconsin.csv", 0.4, "positive", "negative")
  }

  @Test
  def validationTestWithAbaloneDataBase() = {
    validationTest("abalone19.csv", 0.4, "positive", "negative")
  }

  private def validationTest(databaseName: String, expectedValue: Double,
                             minorityClass: String, mayorityClass: String) = {
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

    val AUC = calculateAUC(databaseName, classifier, rows, expectedValue, minorityClass, mayorityClass)

    val GM = calculateGM(databaseName, classifier, rows, expectedValue, minorityClass, mayorityClass)

    println("Precision de la clasificación: " + (countTrue / rows.length).toString + " %")

    assert(AUC > expectedValue)
    assert(GM > expectedValue)
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

    //miner.DISTRIBUTION_EVALUATOR = new Hellinger

    val patterns = miner.mine(training.collect())
    //Minador afectado con la metrica de helinger

    var classifier = InstantiatePBC4cip.buildClassifierWithContrastPatterns(patterns);
    classifier.train()

    val rows = test.collect()
    (classifier, rows)
  }


  def calculateAUC(databaseName: String, classifier: PBC4cip, instances: Array[Row], expectedValue: Double,
                   minorityClass: String, mayorityClass: String): Double = {
    println("Start AUC metric")

    val TPrate = calculateTPrate(classifier, instances, minorityClass)
    val FPrate = calculateFPrate(classifier, instances, mayorityClass)

    val AUC = (1d + TPrate - FPrate) / 2
    println("The AUC metric result is " + AUC)


    println("End AUC metric")
    return AUC
  }


  def calculateGM(databaseName: String, classifier: PBC4cip,
                  instances: Array[Row], expectedValue: Double,
                  minorityClass: String, mayorityClass: String): Double = {
    println("")
    println("Start GM metric");

    val TPrate = calculateTPrate(classifier, instances, minorityClass)
    val TNrate = calculateTNrate(classifier, instances, mayorityClass)

    val GM = Math.sqrt(TPrate * TNrate)
    println("The GM metric result is " + GM)


    println("End GM metric");

    return GM
  }

  def calculateTNrate(classifier: PBC4cip, instances: Array[Row], mayorityClass: String): Double = {

    var countOfObjectsOfTheMayorityClass = 0d
    var instanceMissclassified = 0d

    for (inst <- instances) {
      if (inst.getString(inst.schema.length - 1).equals(mayorityClass))
        countOfObjectsOfTheMayorityClass += 1
    }

    for (inst <- instances) {
      val resultOfClassification = classifier.classify(inst)
      if (resultOfClassification.clazzFeature.isMatch(inst) && inst.getString(inst.schema.length - 1).equals(mayorityClass))
        instanceMissclassified += 1
    }

    return instanceMissclassified / countOfObjectsOfTheMayorityClass
  }


  def calculateTPrate(classifier: PBC4cip, instances: Array[Row], minorityClass: String): Double = {
    var countOfObjectsOfTheMinorityClass = 0d
    var instanceWellClassified = 0d

    for (inst <- instances) {
      if (inst.getString(inst.schema.length - 1).equals(minorityClass))
        countOfObjectsOfTheMinorityClass += 1
    }

    for (inst <- instances) {
      val resultOfClassification = classifier.classify(inst)
      if (resultOfClassification.clazzFeature.isMatch(inst) && inst.getString(inst.schema.length - 1).equals(minorityClass))
        instanceWellClassified += 1
    }

    return instanceWellClassified / countOfObjectsOfTheMinorityClass
  }

  def calculateFPrate(classifier: PBC4cip, instances: Array[Row], mayorityClass: String): Double = {
    var countOfObjectsOfTheMayorityClass = 0d
    var instanceMissclassified = 0d

    for (inst <- instances) {
      if (inst.getString(inst.schema.length - 1).equals(mayorityClass))
        countOfObjectsOfTheMayorityClass += 1
    }

    for (inst <- instances) {
      val resultOfClassification = classifier.classify(inst)
      if (!resultOfClassification.clazzFeature.isMatch(inst) && inst.getString(inst.schema.length - 1).equals(mayorityClass))
        instanceMissclassified += 1
    }

    return instanceMissclassified / countOfObjectsOfTheMayorityClass
  }

  private def doubleComparison(x: Double, y: Double, precision: Double): Boolean = {
    println(y)

    if ((x - y).abs < precision)
      return true
    return false
  }
}