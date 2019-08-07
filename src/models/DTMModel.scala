package models
import common.miner.DatasetSchema
import common.model.Model
import common.pattern.ContrastPattern
import common.utils.{BitArray, PatternToBinaryFeature}
import org.apache.spark.sql.Row

/**
  * Decision Table Majority or Simple Decision Table
  * A. J. Knobbe and E. K. Y. Ho, “Pattern Teams,” in PKDD 2006, pp. 577–
  * 584, 2006.
  */
class DTMModel extends Model {
 private var matrixMaker: PatternToBinaryFeature = _

  /**
    * Initialize the classifier.
    *
    * @param patterns
    * @param trainingInstances
    * @param dataMiner
    */
  override def initialize(patterns: Array[ContrastPattern], trainingInstances: Array[Row], dataMiner: DatasetSchema): Unit = {
    matrixMaker = new PatternToBinaryFeature(patterns, trainingInstances, dataMiner)
    this.patterns = patterns
    this.trainingInstances = trainingInstances
    this.dataMiner = dataMiner
    this.classFeatures = dataMiner.classFeatures()
  }

  /**
    * Method to predict the class of an instance. It returns an
    * Array of Double with the votes of every class.
    *
    * @param instance An instance to be classified
    * @return Array with the votes of every class
    */
  override def predict(instance: Row): Array[Double] = {
    var codeInstances = Array.emptyIntArray
    val row = rowToBitArray(instance)
    val matrix = matrixMaker.matrix

    for (i ← 0 until matrixMaker.rowsCount) {
      var instanceCovered = true
      for(j ← matrix.indices ; if(instanceCovered)){
        if(matrix(j)(i) != row(j))
          instanceCovered = false
      }
      if(instanceCovered) codeInstances = codeInstances :+ i
    }

    val clazzToPredict = matrixMaker.classFeatures.indices.toArray
    clazzToPredict.map(clazz ⇒ dtmClassifier(clazz, codeInstances))
  }

  private def rowToBitArray(row: Row): Array[Boolean] = {
    val patternsMatchIndex = patterns.indices.toArray.filter(i ⇒ patterns(i).isMatch(row))
    val bitArray = new BitArray(patterns.length)
    bitArray.setAllValues(false)
    patternsMatchIndex.foreach(i ⇒ bitArray(i) = true)
    bitArray.toArray()
  }

  private def dtmClassifier(clazz: Int, codeInstances: Array[Int]): Double = {
    val patterns = matrixMaker.getPatternsFromClass(clazz).map(matrixMaker.matrix(_))
    var instances = new BitArray(matrixMaker.rowsCount)
    instances.setAllValues(false)
    patterns.foreach(p ⇒ instances = p or instances)
    val instanceFromPatterns = (0 until matrixMaker.rowsCount).toArray.filter(instances(_))

    if(codeInstances.length > 0)
      (codeInstances intersect instanceFromPatterns).length.toDouble / codeInstances.length.toDouble
    else
      instanceFromPatterns.length.toDouble / matrixMaker.rowsCount.toDouble
  }

  override def toString: String = "DTM"

}
