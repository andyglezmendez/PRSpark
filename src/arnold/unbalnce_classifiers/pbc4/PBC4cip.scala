package classifiers.unbalnce_classifiers.pbc4

import classifiers.unbalnce_classifiers.ImbalanceClassifier
import classifiers.pattern_clasification.ClasificationResult
import common.feature.Feature
import common.pattern.ContrastPattern
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

class PBC4cip extends ImbalanceClassifier {

  /**
    * Weight associate to each class
    */
  var weightPerClass: Array[Double] = null

  /**
    * Total of objects on the dataset
    */
  var totalObjectsOnDataset: Double = 0

  /**
    * Parallel array that contains the support foreach feature class value
    */
  var supportFeatureValueIndex: Array[Double] = null

  /**
    * Parallel array that stores the feature corresponding with the supportFeatureValueIndex array
    */
  var featureValues = new Array[Feature[_]](0)

  override def train(): Unit = {
    super.train();

    initializeParallelArrays();
    calculateTotalObjectsOnDataset();
    calculateSupportPerClass();

    weightPerClass = new Array[Double](featureValues.length)
    for (i <- 0 until weightPerClass.length) {
      weightPerClass(i) = calculateWeight(i)
    }
  }

  override def classify(objToClassify: Row): ClasificationResult = {
    super.classify(null);

    var result: Array[Double] = new Array[Double](weightPerClass.length);
    {
      for (i <- 0 until weightPerClass.length) {

        // Selecciono la feature clazz por la que se le va a intentar clasificar este objeto
        // filtro la coleccion patternCollection por dicha feature clazz
        val featureTarget = this.featureValues(i)
        val patternsThatBelongThatFeature: ListBuffer[ContrastPattern] = new ListBuffer[ContrastPattern]
        patternCollection.foreach((f: ContrastPattern) => if (f.clazz == featureTarget) patternsThatBelongThatFeature += f)

        //Selecciono los patrones que matchean con dicho objeto
        var patternFiltered: ListBuffer[ContrastPattern] = new ListBuffer[ContrastPattern]

        for(pattern <- patternsThatBelongThatFeature){
          if(pattern.isMatch(objToClassify))
            patternFiltered += pattern
        }


        // Ejecuto la ecuacion de clasificacion que es Wc * Sumatoria soporte(Patron i)
        var supportOfPatterns = 0d
        patternFiltered.foreach((pattern: ContrastPattern) => {
          supportOfPatterns += patternSupport(pattern)
        })

        //Ejecuto Wc * acumaulado supportOfPatterns
        val finalResult = supportOfPatterns * weightPerClass(i)
        result(i) = finalResult
      }
    }

    return buildResultObject(result, featureValues,objToClassify);
  }

  /**
    * This method build the result object
    *
    * @param acumulateSupportPerClass An array of double that contains the accumulation of support per class
    * @param featureValues            The possible values of the class target
    * @return An instance of ClasificationResult that contains the support distribution and the
    *         Feature Class that correspond to the classified object
    */
  def buildResultObject(acumulateSupportPerClass: Array[Double], featureValues: Array[Feature[_]],obj:Row): ClasificationResult = {

    var value = Double.MinValue
    var index = -1
    for (i <- 0 until acumulateSupportPerClass.length)
      if (acumulateSupportPerClass(i) > value) {
        value = acumulateSupportPerClass(i)
        index = i
      }

    return new ClasificationResult(acumulateSupportPerClass, featureValues(index),obj)
  }


  /**
    * This method initializes the feature distribution
    */
  private def initializeParallelArrays(): Unit = {

    var list: ListBuffer[Feature[_]] = new ListBuffer[Feature[_]];
    patternCollection.foreach((f: ContrastPattern) => if (!list.contains(f.clazz)) list += f.clazz)

    featureValues = list.toArray
    supportFeatureValueIndex = new Array[Double](featureValues.length)
  }

  /**
    * This method calculates the support foreach class
    */
  private def calculateSupportPerClass(): Unit = {

    for (i <- 0 until featureValues.length) {
      val targetFeature = featureValues(i)

      val patternMatchingWithFeature: ListBuffer[ContrastPattern] = new ListBuffer[ContrastPattern]
      patternCollection.foreach((f: ContrastPattern) => if (f.clazz == targetFeature) patternMatchingWithFeature += f)

      var supportPerClass: Double = 0
      patternMatchingWithFeature.foreach((f: ContrastPattern) => supportPerClass += patternSupport(f))
      supportFeatureValueIndex(i) = supportPerClass
    }
  }

  /**
    * This method claculates the support of the pattern based on the contingency table
    *
    * @param contrastPattern The contrast pattern that we want know the support
    * @return A double that indicates the support of the pattern on the class
    */
  private def patternSupport(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable()
    if (ct.N_nP_C + ct.N_P_C == 0)
      return Double.NaN;

    return ct.N_P_C / (ct.N_nP_C + ct.N_P_C);
  }

  /**
    * This method calculate the total objects on the dataset based on the contingency table
    */
  private def calculateTotalObjectsOnDataset(): Unit = {
    var cp = patternCollection(0)
    val ct = cp.contingencyTable()
    totalObjectsOnDataset = ct.N_nP_nC + ct.N_nP_C + ct.N_P_nC + ct.N_P_C
  }

  /**
    * This method calculates the weight per class
    *
    * @param i index of the feature target
    * @return The weight of the current feature target
    */
  private def calculateWeight(i: Int): Double = {
    var result = 0;

    val featureTarget = featureValues(i)
    val supportOfTheClass: Double = supportFeatureValueIndex(i)
    val totalObjects: Double = this.totalObjectsOnDataset


    var countObjectsOfTheClass: Integer = 0;
    patternCollection.foreach((f: ContrastPattern) => if (f.clazz == featureTarget) countObjectsOfTheClass += 1)

    return (1 - (countObjectsOfTheClass / totalObjects)) / supportOfTheClass;
  }

  /**
    * This method get's a double array that contains the support of each pattern every position on
    * the array match with the position of a contrast pattern on the array patternCollection
    *
    * @return An array with the supports
    */
  def getPatternSupports(): Array[Double] = {
    var listBuffer: ListBuffer[Double] = new ListBuffer[Double]
    patternCollection.foreach((f: ContrastPattern) => listBuffer += patternSupport(f))
    return listBuffer.toArray
  }

  /**
    * Exactly the method getPatternSupports the only difference is that the support of every pattern
    * is rounded
    *
    * @param decimalPlaces The count of decimal places that we want
    * @return An array with the supports rounded
    */
  def getPatternSupportsRounded(decimalPlaces: Integer): Array[Double] = {
    var listBuffer = getPatternSupports()
    for (i <- 0 until listBuffer.length) {
      listBuffer(i) = roundAnyDecimalPlace(listBuffer(i), decimalPlaces)
    }

    return listBuffer.toArray
  }

  /**
    * This method get's the weight per class
    *
    * @return An array that contains the weight per class
    */
  def getWeightPerClass: Array[Double] = {
    return this.weightPerClass.clone();
  }

  /**
    * This method get's the weight of each class rounded
    *
    * @param decimals The count of decimal places
    * @return A double array that contains the supports rounded
    */
  def getWeightPerClassRounded(decimals: Integer): Array[Double] = {
    val result: Array[Double] = new Array[Double](weightPerClass.length);

    for (i <- 0 until weightPerClass.length)
      result(i) = roundAnyDecimalPlace(weightPerClass(i), decimals);

    return result
  }

  /**
    * This method round a decimal number
    *
    * @param number      Number that we want round
    * @param countPlaces Count of decimal places that we want
    * @return The number rounded
    */
  def roundAnyDecimalPlace(number: Double, countPlaces: Integer): Double = {

    val  decimalPlace = Math.pow(10,countPlaces.toDouble);

    var result = number * decimalPlace
    result = result.round
    return result / decimalPlace
  }
}
