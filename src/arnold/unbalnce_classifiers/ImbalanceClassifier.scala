package classifiers.unbalnce_classifiers

import classifiers.pattern_clasification.ClasificationResult
import common.pattern.ContrastPattern
import org.apache.hadoop.hive.metastore.api.InvalidOperationException
import org.apache.spark.sql.Row

/**
  * Esta es la interfaz para los clasificadores que pueden lidiar con el
  * desbalance de clases
  */
trait ImbalanceClassifier extends Serializable {

  /**
    * Pattern collection
    */
  var patternCollection: Array[ContrastPattern] = _;

  /**
    * Indicates if the classifier was initialized
    */
  var classifierInitialized = false

  /**
    * Indicate if the classifier was trained
    */
  var trainedClassifier = false

  /**
    * This method initialize the classifier
    *
    * @param patterns Set of contrast patterns
    */
  def initializeClassifier(patterns: Array[ContrastPattern]): Unit = {
    this.patternCollection = patterns;
    classifierInitialized = true;
  }


  /**
    * Training stage
    * This method must be call on the first instruction line after does override.
    * If this method is not be called then is trowed an InvalidOperationException that contains the next
    * label: "The classifier must be initialized first"
    */
  def train(): Unit = {
    if (!classifierInitialized)
      throw new InvalidOperationException("The classifier must be initialized first")
    trainedClassifier = true
  };

  /**
    * Clasification stage
    * This method must be call on the first instruction line after does override.
    * If this method is not be called then is trowed an InvalidOperationException that contains the next
    * label: "TThe classifier must be trained first"
    *
    * @return A double array that contains the class classification per class. The highest value of the array is the
    *         assigned class to the object
    */
  def classify(objectToClassify: Row): ClasificationResult = {
    if (!trainedClassifier)
      throw new InvalidOperationException("The classifier must be trained first");
    return null;
  }


}
