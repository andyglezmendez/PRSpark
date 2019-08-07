package common.miner

import common.pattern.ContrastPattern
import org.apache.spark.sql.{Column, Row}

trait SparkMiner extends Serializable{

  var dataMiner: DatasetSchema

  def mine(rows: Array[Row]): Array[ContrastPattern]

  /**
    * Validate the arguments passed to the miner
    * Must throw a new IllegalArgumentException if the arguments are not valid
    */
  protected def validateArguments():Unit
}
