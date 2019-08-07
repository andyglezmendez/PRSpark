package common.dataframe.dicretizer

import common.miner.DatasetSchema
import common.utils.MathLog.logB
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ArrayBuffer

class EntropyBasedDiscretizer extends Discretizer {

  var dataMiner: DatasetSchema = _

  def calculateEntropy(distribution: Array[Double]): Double = {
    val total = distribution.sum
    distribution.map(_ / total).map { value ⇒ if (value > 0) -value * (logB(value, 2)) else 0 }.sum
  }

  def calculateClassInformationEntropy(currentDistribution: Array[Array[Double]]): Double = {
    val total = currentDistribution(0).sum + currentDistribution(1).sum
    currentDistribution.map { dist ⇒ dist.sum / total * calculateEntropy(dist) }.sum
  }

  def canCutAccordingToMDL(entropy: Double, distribution: Array[Array[Double]]): Boolean = {
    val sDist = distribution(0) ++ distribution(1)
    val n = sDist.sum

    val k = sDist.count(_ > 0)
    val k1 = distribution(0).count(_ > 0)
    val k2 = distribution(1).count(_ > 0)

    val sEntropy = calculateEntropy(sDist)
    val delta = logB(math.pow(3, k) - 2, 2) -
      (k * sEntropy - k1 * calculateEntropy(distribution(0))
        - k2 * calculateEntropy(distribution(1)))

    val gain = sEntropy - entropy

    gain > logB(n - 1, 2) / n + delta / n
  }

  def findCutPoint(instances: Array[Row], col: Int): ArrayBuffer[Double] = {
    var cutPoints: ArrayBuffer[Double] = new ArrayBuffer[Double]()

    val iter = new OrderedColumnSplitIterator
    val membership = instances.map {
      (_, 1d)
    }
    iter.initialize(col, membership, dataMiner)
    var minEntropy = Double.MaxValue
    var cutPoint = Double.NaN
    var minDistribution: Array[Array[Double]] = null

    while (iter.next()) {
      val entropy = calculateClassInformationEntropy(iter.currentDistribution)
      if (entropy < minEntropy) {
        minEntropy = entropy
        cutPoint = iter.selectorFeatureValue
        minDistribution = iter.currentDistribution.clone()
      }
    }
    if (minDistribution != null && canCutAccordingToMDL(minEntropy, minDistribution)) {
      cutPoints += cutPoint
      val notNullInstances = instances.filter(!_.isNullAt(col))
      cutPoints ++= findCutPoint(notNullInstances.filter(_.getDouble(col) <= cutPoint), col)
      cutPoints ++= findCutPoint(notNullInstances.filter(_.getDouble(col) > cutPoint), col)
    }

    cutPoints
  }

  // This is for all Discretizers

  def discretize(data: DataFrame): DataFrame = {
    val colsToDiscretize = data.schema.filter(sf ⇒ sf.dataType == DoubleType).map { sf ⇒ (sf, data.schema.indexOf(sf)) }
    val className = data.schema.last.name
    var ds = data
    dataMiner = new DatasetSchema(data.collect())

    for ((sf, col) ← colsToDiscretize) {
      val discretizer = new EntropyBasedDiscretizer
      discretizer.dataMiner = dataMiner
      var cutPoints = discretizer.findCutPoint(ds.collect(), col).sorted
      cutPoints = Double.NegativeInfinity +: cutPoints :+ Double.PositiveInfinity
      val transform = udf { value: String ⇒ {
        var result = value
        if (value != null)
          for (i ← 0 until cutPoints.length - 1) {
            if (value.toDouble >= cutPoints(i) && value.toDouble < cutPoints(i + 1))
              result = s"[${cutPoints(i)}; ${cutPoints(i + 1)})"
          }
        result
      }
      }
      ds = ds.withColumn(s"D_${sf.name}", transform(ds.col(sf.name)))
    }
    colsToDiscretize.foreach(tuple ⇒ ds = ds.drop(tuple._1.name))

    val copy = udf { x: String ⇒ x }
    ds = ds.withColumn("new_class", copy(ds.col(className)))
    ds = ds.drop(className)
    ds = ds.withColumnRenamed("new_class", className)

    ds
  }

  def discretize(data: DataFrame, cols: Array[String]): DataFrame ={
    val colsToDiscretize = data.schema.filter(sf ⇒ cols.exists(_.equalsIgnoreCase(sf.name))).map { sf ⇒ (sf, data.schema.indexOf(sf)) }
    val className = data.schema.last.name
    var ds = data
    dataMiner = new DatasetSchema(data.collect())

    for ((sf, col) ← colsToDiscretize) {
      val discretizer = new EntropyBasedDiscretizer
      discretizer.dataMiner = dataMiner
      var cutPoints = discretizer.findCutPoint(ds.collect(), col).sorted
      cutPoints = Double.NegativeInfinity +: cutPoints :+ Double.PositiveInfinity
      val transform = udf { value: String ⇒ {
        var result = value
        if (value != null)
          for (i ← 0 until cutPoints.length - 1) {
            if (value.toDouble >= cutPoints(i) && value.toDouble < cutPoints(i + 1))
              result = s"[${cutPoints(i)}; ${cutPoints(i + 1)})"
          }
        result
      }
      }
      ds = ds.withColumn(s"D_${sf.name}", transform(ds.col(sf.name)))
    }
    colsToDiscretize.foreach(tuple ⇒ ds = ds.drop(tuple._1.name))

    val copy = udf { x: String ⇒ x }
    ds = ds.withColumn("new_class", copy(ds.col(className)))
    ds = ds.drop(className)
    ds = ds.withColumnRenamed("new_class", className)

    ds
  }
}
