package common.utils

import common.dataframe.dicretizer.IntegerToStringDiscretizer
import common.filter.pattern_teams_measures.AUCMeasure
import common.miner.DatasetSchema
import common.serialization.ARFFSerializer
import miners.pr_framework.miner.RandomForestMiner
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.Test

class PatternToBinaryFeatureTest {


  @Test
  def test(): Unit = {
    val conf = new SparkConf().setMaster("local")
      .setAppName("PR-Spark-Testing")
      .set("spark.sql.warehouse.dir", "..\\")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val ser = new ARFFSerializer()
    val rows = spark.sparkContext.parallelize(ser.loadRowsFromARFF("D:\\Home\\School\\Data Mining\\BD\\glass.arff"))
    val dm = new DatasetSchema(rows.collect())
    val a = spark.createDataFrame(rows, rows.first().schema)

    val df = new IntegerToStringDiscretizer().discretize(a)
    val miner = new RandomForestMiner()
    val patterns = miner.mine(df.collect())

    val m = new PatternToBinaryFeature(patterns, df.collect(), new DatasetSchema(df.collect()))
//    m.matrix.foreach(println)

    val auc = new AUCMeasure
    val p1 = patterns

//    auc.getMeasure(p1, df.collect().length)

    val n = m.matrix(0).count(b⇒ b)
    println(m.matrix(0))
    println(n)
  }




}
