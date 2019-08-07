package miners.epm.miner

import java.util

import algorithms.topk
import common.miner.{DatasetSchema, SparkMiner}
import common.pattern.ContrastPattern
import framework.utils.Utils
import miners.epm.SparkToEPM
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

//Only Nominal Features
class TopKMiner extends SparkMiner {
  override var dataMiner: DatasetSchema = _

  var K = 10

  override def mine(rows: Array[Row]): Array[ContrastPattern] = {
    dataMiner = new DatasetSchema(rows)
    val wrapper = new SparkToEPM(rows, dataMiner)

    val miner = new topk.TopK
    val params = new util.HashMap[String, String]()
    params.put("K",K.toString)

    miner.learn(wrapper.instanceSet, params)
    val map = Utils.calculateDescriptiveMeasures(wrapper.instanceSet, miner.getPatterns, true)
    val patterns = wrapper.patternToContrastPattern(miner.getPatterns.asScala.toArray)
    patterns.foreach(pattern ⇒ pattern.contingencyCalc(rows))
    patterns
  }

  override protected def validateArguments(): Unit = {
    if (K < 1)
      throw new IllegalArgumentException("K must be greater than 1")
  }
}
