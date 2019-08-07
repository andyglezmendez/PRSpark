package miners.epm.miner

import java.util

import algorithms.iepminer
import common.miner.{DatasetSchema, SparkMiner}
import common.pattern.ContrastPattern
import framework.utils.Utils
import miners.epm.SparkToEPM
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

/**
  * Only accept nominal features
  */
class IEPMiner extends SparkMiner{
  override var dataMiner: DatasetSchema = _

  var MIN_SUPPORT = 0.3
  var MIN_GROWTH_RATE = 2.0
  var MIN_CHI_SQUARED = 3.84

  override def mine(rows: Array[Row]): Array[ContrastPattern] = {
    dataMiner = new DatasetSchema(rows)
    val wrapper = new SparkToEPM(rows, dataMiner)

    val miner = new iepminer.IEPMiner
    val params = new util.HashMap[String, String]()
    params.put("Minimum Support", MIN_SUPPORT.toString)
    params.put("Minimum Growth Rate", MIN_GROWTH_RATE.toString)
    params.put("Minimum Chi-Squared", MIN_CHI_SQUARED.toString)

    miner.learn(wrapper.instanceSet, params)
    val map = Utils.calculateDescriptiveMeasures(wrapper.instanceSet, miner.getPatterns,true)
    val patterns = wrapper.patternToContrastPattern(miner.getPatterns.asScala.toArray)
    patterns.foreach(pattern ⇒ pattern.contingencyCalc(rows))
    patterns
  }

  override protected def validateArguments(): Unit = {
    if (MIN_SUPPORT < 0 || MIN_SUPPORT > 1)
      throw new IllegalArgumentException("MIN_SUPPORT must be [0,1]")
    if (MIN_GROWTH_RATE < 1 || MIN_GROWTH_RATE > 10000)
      throw new IllegalArgumentException("MIN_GROWTH_RATE must be [1,10000]")
    if (MIN_CHI_SQUARED <0 || MIN_CHI_SQUARED > 10000)
      throw new IllegalArgumentException("MIN_CHI_SQUARED must be [1,10000]")

  }
}
