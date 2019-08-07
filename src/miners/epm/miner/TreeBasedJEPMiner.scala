package miners.epm.miner

import java.util

import algorithms.tree_based_jep
import common.miner.{DatasetSchema, SparkMiner}
import common.pattern.ContrastPattern
import framework.utils.Utils
import miners.epm.SparkToEPM
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

class TreeBasedJEPMiner extends SparkMiner {
  override var dataMiner: DatasetSchema = _

  var ALPHA = 0.3
  var PATTERN_MAX_LENGTH = -1
  var ORDER = TreeBasedJEPMiner.ORDER_MODE_HYBRID

  override def mine(rows: Array[Row]): Array[ContrastPattern] = {
    dataMiner = new DatasetSchema(rows)
    val wrapper = new SparkToEPM(rows, dataMiner)

    val miner = new tree_based_jep.TreeBasedJEP
    val params = new util.HashMap[String, String]()
    params.put("Alpha", ALPHA.toString)
    params.put("Pattern Max Length", PATTERN_MAX_LENGTH.toString)
    params.put("Ordering", ORDER)

    miner.learn(wrapper.instanceSet, params)
    val map = Utils.calculateDescriptiveMeasures(wrapper.instanceSet, miner.getPatterns, true)
    val patterns = wrapper.patternToContrastPattern(miner.getPatterns.asScala.toArray)
    patterns.foreach(pattern ⇒ pattern.contingencyCalc(rows))
    patterns
  }

  override protected def validateArguments(): Unit = {
    if (ALPHA < 0 || ALPHA > 1)
      throw new IllegalArgumentException("ALPHA must be [0,1]")
    if (PATTERN_MAX_LENGTH < -1)
      throw new IllegalArgumentException("PATTERN_MAX_LENGTH must be -1 for no limit or [0,999999]")
  }
}

object TreeBasedJEPMiner {
  val ORDER_MODE_FREQUENCY = "frequency"
  val ORDER_MODE_RATIO = "ratio"
  val ORDER_MODE_RATIO_INVERSE = "ratioInverse"
  val ORDER_MODE_LPNC = "LPNC"
  val ORDER_MODE_MPPC = "MPPC"
  val ORDER_MODE_HYBRID = "hybrid"
}
