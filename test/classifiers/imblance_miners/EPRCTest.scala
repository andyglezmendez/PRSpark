package classifiers.imblance_miners

import arnold.imbalance_miners.EPRCMiner
import common.feature.{CategoricalFeature, Feature}
import common.pattern.ContrastPattern
import miners.pr_framework.miner.RandomForestMiner
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.Test

class EPRCTest {

  var baseMiner: RandomForestMiner = null;
  var miner: EPRCMiner = null;

  def initializeMiner(): Unit = {
    baseMiner = new RandomForestMiner();
  }

  @Test
  def initializeClassifierWithContrastPatterns(): Unit = {
    val minorityClass: Feature[String] = new CategoricalFeature("Class", 9, "positive")
    val mayorityClass: Feature[String] = new CategoricalFeature("Class", 9, "negative")
    val p = validateMiner("glass1.csv", minorityClass, mayorityClass, 1000);

    val c = p._1
    val cpInitial = p._2

    assert(c.length > 0)
    println("Cantidad patrones de minados -- " + c.length)
    println("Cantidad de patrones iniciales -- "  + cpInitial)
  }

  private def validateMiner(databaseName: String, minorityClass: Feature[String],
                            mayorityClass: Feature[String], growRate: Integer)
  : (Array[ContrastPattern], Integer) = {

    val rows: Array[Row] = initializeSparkEnviroment(databaseName)
    miner = new EPRCMiner(new RandomForestMiner(), minorityClass, mayorityClass, growRate);
    return (miner.mine(rows), miner.initialCountCP);
  }

  private def initializeSparkEnviroment(databaseName: String): Array[Row] = {
    val conf = new SparkConf().setMaster("local")
      .setAppName("My App")
      .set("spark.sql.warehouse.dir", "..\\")

    val ss = SparkSession.builder().config(conf).getOrCreate()

    var ds = ss.sqlContext.read.format("csv").option("header", "true").load("resource\\" + databaseName)
    //ds.cache()

    return ds.collect()
  }
}
