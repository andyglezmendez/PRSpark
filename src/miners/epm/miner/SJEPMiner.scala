package miners.epm.miner

import java.util

import algorithms.sjep_classifier
import common.miner.{DatasetSchema, SparkMiner}
import common.pattern.ContrastPattern
import framework.utils.Utils
import miners.epm.SparkToEPM
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

//Only Nominal Features
class SJEPMiner extends SparkMiner{
  override var dataMiner: DatasetSchema = _

  var MIN_SUPPORT = 0.05

  override def mine(rows: Array[Row]): Array[ContrastPattern] = {
    dataMiner = new DatasetSchema(rows)
    val wrapper = new SparkToEPM(rows, dataMiner)

    val miner = new sjep_classifier.SJEP_Classifier
    val params = new util.HashMap[String, String]()
    params.put("Min Support", MIN_SUPPORT.toString)

    miner.learn(wrapper.instanceSet, params)
    val map = Utils.calculateDescriptiveMeasures(wrapper.instanceSet, miner.getPatterns, true)
    val patterns = wrapper.patternToContrastPattern(miner.getPatterns.asScala.toArray)
    patterns.foreach(pattern ⇒ pattern.contingencyCalc(rows))
    patterns
  }

  override protected def validateArguments(): Unit = {
      if (MIN_SUPPORT < 0 || MIN_SUPPORT > 1)
      throw new IllegalArgumentException("MIN_SUPPORT must be [0,1]")
  }
}
