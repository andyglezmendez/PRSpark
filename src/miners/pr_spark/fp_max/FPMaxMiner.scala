package miners.pr_spark.fp_max

import common.miner.{DatasetSchema, SparkMiner}
import common.pattern.ContrastPattern
import org.apache.spark.sql.Row

import scala.collection.mutable.ArrayBuffer

class FPMaxMiner extends SparkMiner {

  override var dataMiner: DatasetSchema = _

  var MIN_SUPPORT = 0.1

  override def mine(rows: Array[Row]): Array[ContrastPattern] = {
    validateArguments()
    dataMiner = new DatasetSchema(rows)
    var patterns = new ArrayBuffer[ContrastPattern]()
    val interop = new FPMaxInterop(rows, dataMiner, MIN_SUPPORT)

    val frequentItems = interop.itemsByDistribution
    val minSupports = interop.minSupportByDistribution
    val transactions = interop.transactions


    for (i ← transactions.indices){
      val extractor = new FPMaxItemsetExtractor
      val itemsets = extractor.extractMaximaItemsets(frequentItems(i), transactions(i), minSupports(i))
      patterns ++= interop.extractContrastPatterns(itemsets.toArray, i)
    }

    patterns.foreach(_.contingencyCalc(rows))
    patterns.toArray
  }

  override protected def validateArguments(): Unit = {
    if (MIN_SUPPORT < 0 || MIN_SUPPORT > 1)
      throw new IllegalArgumentException("MIN_SUPPORT must be [0,1]")
  }
}
