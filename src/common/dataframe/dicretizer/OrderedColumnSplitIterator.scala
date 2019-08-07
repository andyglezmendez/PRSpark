package common.dataframe.dicretizer

import common.feature.CategoricalFeature
import common.miner.DatasetSchema
import org.apache.spark.sql.Row

class OrderedColumnSplitIterator {

  var instances: Array[(Row, Double)] = _
  var initialized = false
  var col: Int = -1
  var currentDistribution: Array[Array[Double]] = new Array[Array[Double]](2)
  var sorted: Array[(Row, Double)] = _
  var currentIndex = -1
  var lastClassValue: String = _
  var selectorFeatureValue: Double = _


  var classFeatures: Array[(CategoricalFeature, Int)] = _
  var classIndex: Int = _


  def initialize(col: Int, instances: Array[(Row, Double)], dataMiner: DatasetSchema) = {
    this.col = col
//    println(s"MINGA $col")
    this.instances = instances
    classFeatures = dataMiner.classFeatures()

    sorted = instances.filter(!_._1.isNullAt(col)).sortBy(tuple ⇒ tuple._1.getDouble(col))
//    sorted = instances.sortBy(tuple ⇒ {
//      if (tuple._1.isNullAt(col))
//        tuple._1.getDouble(col)
//      else 0
//    })
    //    sorted = instances.sortBy(tuple ⇒ tuple._1.getDouble(col))

    currentDistribution(0) = new Array[Double](classFeatures.length)
    currentDistribution(1) = classFeatures.map(tuple ⇒ tuple._2.toDouble)
    classIndex = dataMiner.columnsCount - 1 //sorted.head._1.length - 1

    if(sorted.length != 0)
      lastClassValue = findNextClass(0)
    initialized = true

  }

  def findNextClass(index: Int): String = {
    val currentClass = sorted(index)._1.getString(classIndex)
    val currentValue = sorted(index)._1.getDouble(col)

    var nextIndex = index + 1
    while (nextIndex < sorted.length && currentValue.equals(sorted(nextIndex)._1.getDouble(col))) {
      if (sorted(nextIndex)._1.getString(classIndex) != currentClass)
        return ""
      nextIndex += 1
    }
    return currentClass
  }

  def next(): Boolean = {
    if (!initialized) throw new IllegalStateException("Iterator not initialized")
    if (currentIndex >= sorted.length - 1) return false

    currentIndex += 1
    for (actualIndex ← currentIndex until sorted.length - 1) {
      val instance = sorted(actualIndex)._1;
      val objClass = classFeatures.indexWhere(tuple ⇒ tuple._1.value.equals(instance.getString(classIndex)))

      currentDistribution(0)(objClass) += sorted(actualIndex)._2
      currentDistribution(1)(objClass) -= sorted(actualIndex)._2

      if (instance.getDouble(col) != sorted(actualIndex + 1)._1.getDouble(col)) {

        val nextClassValue = findNextClass(actualIndex + 1);
        if (lastClassValue != nextClassValue || (lastClassValue == "" && nextClassValue == "")) {
          //          if (CuttingStrategy == CuttingStrategy.OnPoint)
          //            _selectorFeatureValue = instance[_feature];
          //          else
          selectorFeatureValue = (instance.getDouble(col) + sorted(actualIndex + 1)._1.getDouble(col)) / 2;
          lastClassValue = nextClassValue;
          currentIndex = actualIndex
          return true;
        }
      }
      currentIndex = actualIndex
    }

    return false;
  }

}
