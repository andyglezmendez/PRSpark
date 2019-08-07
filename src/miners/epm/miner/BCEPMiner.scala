package miners.epm.miner

import java.util

import algorithms.bcep.BCEP_Model
import common.miner.{DatasetSchema, SparkMiner}
import common.pattern.ContrastPattern
import miners.epm.SparkToEPM
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._


class BCEPMiner extends SparkMiner {
  override var dataMiner: DatasetSchema = _

  var MIN_SUPPORT = 0.01
  var MIN_GROWTH_RATE = 100.0

  override def mine(rows: Array[Row]): Array[ContrastPattern] = {
    validateArguments()
    dataMiner = new DatasetSchema(rows)
    val wrapper = new SparkToEPM(rows, dataMiner)

    val miner = new BCEP_Model
    val params = new util.HashMap[String, String]()
    params.put("Minimum support", MIN_SUPPORT.toString)
    params.put("Minimum GrowthRate", MIN_GROWTH_RATE.toString)

    miner.learn(wrapper.instanceSet, params)
    val patterns = wrapper.patternToContrastPattern(miner.getPatterns.asScala.toArray)
    patterns.foreach(pattern ⇒ pattern.contingencyCalc(rows))
    patterns
  }

  override protected def validateArguments(): Unit = {
    if (MIN_SUPPORT < 0 || MIN_SUPPORT > 1)
      throw new IllegalArgumentException("MIN_SUPPORT must be [0,1]")
    if (MIN_GROWTH_RATE < 1 || MIN_GROWTH_RATE > 10000)
      throw new IllegalArgumentException("MIN_GROWTH_RATE must be [1,10000]")
  }
}
