package common.filter

import common.dataframe.dicretizer.EntropyBasedDiscretizer
import common.filter.chosen_few_measures.AgglomerativeClustering
import common.miner.evaluator.CrossValidationEvaluator
import common.pattern.ContrastPattern
import common.serialization.ARFFSerializer
import miners.pr_framework.miner.RandomForestMiner
import models.DTMModel
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.junit.Test

class ChosenFewSetConstraintTest {

  var spark: SparkSession = _
  var sql: SQLContext = _
  var dbName: String = _

  def initializeSpark () = {
    val conf = new SparkConf().setMaster("local")
      .setAppName("PR-Spark-Testing")
      .set("spark.sql.warehouse.dir", "..\\")

    spark = SparkSession.builder().config(conf).getOrCreate()
    sql = spark.sqlContext
  }

  def loadDatabase(path: String): DataFrame ={
    val serializer = new ARFFSerializer()
    val rows = serializer.loadRowsFromARFF(path)
    val rdd = spark.sparkContext.parallelize(rows)
    val df = spark.createDataFrame(rdd, rdd.first().schema)
    df.cache()
    df
  }

  val db = "iris.arff"
  val path = "test_resources\\arff\\"
  val splitEvaluate = 2

  //PRS
  @Test
  def test_prs_dtm(): Unit = {
    initializeSpark()
    var ds = loadDatabase(path + db)
    val discretizer = new EntropyBasedDiscretizer
    ds = discretizer.discretize(ds)

    ds.cache()
    ds.printSchema()

    val cross = new CrossValidationEvaluator(ds, new RandomForestMiner, new DTMModel)
    cross.evaluate(splitEvaluate)
    println(s"Accuracy => ${cross.effectiveness()}")

  }



  @Test
  def test(): Unit ={
    initializeSpark()
    var ds = loadDatabase(path + db)
    val discretizer = new EntropyBasedDiscretizer
    ds = discretizer.discretize(ds)

    val miner = new RandomForestMiner
    val patterns = miner.mine(ds.collect())

    val filter = new ChosenFewSetConstraint(ds.collect())
    filter.order = {cp ⇒ cp.growthRate()}
    val p1 = filter.getPatternSet(patterns, 0.01)
    val p2 = filter.getPatternSet(patterns, 0.2)
    println(s"Trheshold 0.1 => ${p1.length}")
    p1.foreach(println)
    println("")
    println(s"Trheshold 0.6 => ${p2.length}")
    p2.foreach(println)

  }


}
