package miners.epm.miner

import java.util

import algorithms.deeps
import common.miner.{DatasetSchema, SparkMiner}
import common.pattern.ContrastPattern
import framework.utils.Utils
import miners.epm.SparkToEPM
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

//Test this miner, Could be working wrong
class DeEPSMiner extends SparkMiner {
  override var dataMiner: DatasetSchema = _

  var ALPHA = 0.12

  override def mine(rows: Array[Row]): Array[ContrastPattern] = {
    validateArguments()
    dataMiner = new DatasetSchema(rows)
    val wrapper = new SparkToEPM(rows, dataMiner)

    val miner = new deeps.DeEPS_Wrapper
    val params = new util.HashMap[String, String]()
    params.put("ALPHA", ALPHA.toString)

    miner.learn(wrapper.instanceSet, params)
    val map = Utils.calculateDescriptiveMeasures(wrapper.instanceSet, miner.getPatterns, true)
    val patterns = wrapper.patternToContrastPattern(miner.getPatterns.asScala.toArray)
    patterns.foreach(pattern ⇒ pattern.contingencyCalc(rows))
    patterns
  }

  override protected def validateArguments(): Unit = {
    if (ALPHA < 0 || ALPHA > 1)
      throw new IllegalArgumentException("ALPHA must be [0,1]")
  }
}
