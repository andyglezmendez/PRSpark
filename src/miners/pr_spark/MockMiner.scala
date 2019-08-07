package miners.pr_spark

import common.miner.{DatasetSchema, SparkMiner}
import common.pattern.ContrastPattern
import org.apache.spark.sql.Row

/**
  * Miner's mock.
  * @param patterns Patterns to return when mine.
  */
class MockMiner(var patterns: Array[ContrastPattern]) extends SparkMiner{
  override var dataMiner: DatasetSchema = _

  override def mine(rows: Array[Row]): Array[ContrastPattern] = {
    dataMiner = new DatasetSchema(rows)
    patterns
  }

  /**
    * Validate the arguments passed to the miner
    * Must throw a new IllegalArgumentException if the arguments are not valid
    */
  override protected def validateArguments(): Unit = Unit
}
