package common.model

import common.feature.CategoricalFeature
import common.miner.DatasetSchema
import common.pattern.ContrastPattern
import org.apache.spark.sql.Row

/**
  * Interface of model. Sometimes is called Classifier.
  * Is used to classified an instance (Row).
  */
trait Model extends Serializable{

  /**
    * Array of ContrastPatterns used to predict an instance's class.
    */
  protected var patterns: Array[ContrastPattern] = _

  /**
    * Training instances used to create the patterns.
    */
  protected var trainingInstances: Array[Row] = _

  /**
    * The metadata about the training instances.
    */
  protected var dataMiner: DatasetSchema = _

  /**
    * Array with the possible classes of model.
    */
  protected var classFeatures: Array[(CategoricalFeature,Int)] = _

  /**
    * Initialize the classifier.
    * @param patterns
    * @param trainingInstances
    * @param dataMiner
    */
  def initialize(patterns: Array[ContrastPattern], trainingInstances: Array[Row], dataMiner: DatasetSchema)

  /**
    * Method to predict the class of an instance. It returns an
    * Array of Double with the votes of every class.
    * @param instance An instance to be classified
    * @return Array with the votes of every class
    */
  def predict(instance: Row): Array[Double]

  /**
    * Given a prediction maked by the method predict(instance: Row)
    * return the class that correspond with the highest value.
    * @param prediction Array of double returned by predict with the model.
    * @return The class feature that correspond with the highest value in prediction param.
    */
  def getPredictionClass(prediction: Array[Double]): CategoricalFeature = {
    val i = prediction.indexOf(prediction.max)
    classFeatures(i)._1
  }

  def contrastPatterns(): Array[ContrastPattern] = patterns.clone()

  def instances(): Array[Row] = trainingInstances.clone()

  def datasetSchema: DatasetSchema = dataMiner

  def getClassFeatures: Array[(CategoricalFeature, Int)] = classFeatures.clone()

}
