import common.feature._
import common.miner.DatasetSchema
import common.pattern.ContrastPattern
import miners.pr_framework.miner.RandomForestMiner
import miners.pr_spark.fp_max.FPMaxMiner
import models.iCAEPModel
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.junit.Test

class TestALL {

  @Test
  def mainA(): Unit = {
    val f: GreaterEqualThan = new IntegerFeature("a",1,1,true) with GreaterEqualThan
    val f1: EqualThan = new IntegerFeature("a",1,2) with EqualThan

    val f2: LessEqualThan = new IntegerFeature("b",1,3, true) with LessEqualThan
    val f4: EqualThan = new IntegerFeature("b",1,5) with EqualThan
    val f3: EqualThan = new IntegerFeature("b",1,2) with EqualThan

    val a = 12.99

    println(new Array[Double](3).length)

    println(f.isInstanceOf[EqualThan])
    println(f1.isInstanceOf[EqualThan])
    println(f2 contain f4)
  }

  @Test
  def main(): Unit = {
    val conf = new SparkConf().setMaster("local")
      .setAppName("My App")
      .set("spark.sql.warehouse.dir", "..\\")

    val ss = SparkSession.builder().config(conf).getOrCreate()
    val data = ss.read.option("header", "true").csv("resource\\matrix.csv")

    import org.apache.spark.sql._
    var csv = ss.sqlContext.read.format("csv").option("header", "true").load("resource\\view")
    csv.cache()

    val dUDF = udf { s: String => s.toString }
    val dUDFString = udf { s: String => s.toString }

    var ds: Dataset[Row] = csv
    ds.cache()
    //    var i = 0
    //    var size = ds.schema.length
    //    var name = ds.schema.fieldNames
    //
    //    while (i < size - 1) {
    //      var str = name(i) + "D"
    //      var str1 = name(i)
    //      ds = ds.withColumn(str, dUDF(ds.col(str1)))
    //      ds = ds.drop(str1)
    //      i = i + 1
    //    }
    //    ds = ds.withColumn(s"${name(size - 1)}S", dUDFString(ds.col(name(size - 1))))
    //    ds = ds.drop(name(size - 1))

    //    val Array(training, test) = ds.randomSplit(Array(0.7,0.3))
    //
    //    training.cache()
    //    test.cache()
    //    val miner = new FPMaxMiner
    //    miner.MIN_SUPPORT = 0.2
    //    val patterns =  miner.mine(training.collect())
    //
    //    val tra = training.collect().length
    //    println(s"Total ${patterns.length}")
    //    println(s"Instancias ${tra}")
    //    println(patterns.filter(p ⇒ (p.pp + p.pn + p.np + p.nn) != tra).length)
    //
    //    val classifier = new NaiveBayesEPModel(patterns,training.collect(), new DatasetSchema(training))

    //    val parser  = new KeelParser("resource\\glass1.dat")
    //    parser.parseDatabase()
    //    val rows = parser.getRowList().toArray
    //    val qq = ss.sparkContext.parallelize(rows)
    //    ds = ss.createDataFrame(qq, qq.first().schema)


    val results = new Array[Double](1)

    for(i ← results.indices){
      val Array(training, test) = ds.randomSplit(Array(0.7,0.3))

      training.cache()
      test.cache()
      val miner = new RandomForestMiner
      //      miner.MIN_SUPPORT = 0.2
      val patterns =  miner.mine(training.collect())

      val classifier = new iCAEPModel()
      classifier.initialize(patterns,training.collect(), new DatasetSchema(training))
      var good = 0
      var min = 0
      println(s"MIN COUNT")
      var newTest = test.collect()
      for(instance ← newTest){
        min += 1
        val predict = classifier.predict(instance)
        val clas = classifier.getPredictionClass(predict)
        if(clas.isMatch(instance))
          good += 1
      }
      results(i) = good.toDouble/newTest.length.toDouble * 100
      println(s"Good $good from ${newTest.length}")
    }

    val precision: Double = results.sum / results.length
    println(s"La precision es de: ${precision}%")

    //    patterns.foreach(println)

    //    qualities(patterns)
    //    borders(ds, patterns)
  }

  def executeMiner(ds: Dataset[Row]): RDD[ContrastPattern] = {
    //PRSpark Miners
    ds.rdd.mapPartitions(rows ⇒ new FPMaxMiner().mine(rows.toArray).iterator)

    //PRFramework Miners
    //        ds.rdd.mapPartitions(rows ⇒ new RandomForestMiner().mine(rows.toArray).iterator)
    //    ds.rdd.mapPartitions(rows ⇒ new LCMiner().mine(rows.toArray).iterator)
    //    ds.rdd.mapPartitions(rows ⇒ new BaggingMiner().mine(rows.toArray).iterator)
    //    ds.rdd.mapPartitions(rows ⇒ new RandomSplitMiner().mine(rows.toArray).iterator)
    //    ds.rdd.mapPartitions(rows ⇒ new RandomSubsetMiner().mine(rows.toArray).iterator)
    //    ds.rdd.mapPartitions(rows ⇒ new DeleteBetterFeatureMiner().mine(rows.toArray).iterator)
    //    ds.rdd.mapPartitions(rows ⇒ new DeleteBestConditionMiner().mine(rows.toArray).iterator)
    //    ds.rdd.mapPartitions(rows ⇒ new DeleteBestConditionByLevelMiner().mine(rows.toArray).iterator)
    //    ds.rdd.mapPartitions(rows ⇒ new CEPMMiner().mine(rows.toArray).iterator)

    //EPM Miners
    //    ds.rdd.mapPartitions(rows ⇒ new IEPMiner().mine(rows.toArray).iterator)
    //    ds.rdd.mapPartitions(rows ⇒ new BCEPMiner().mine(rows.toArray).iterator)
    //    ds.rdd.mapPartitions(rows ⇒ new DeEPSMiner().mine(rows.toArray).iterator) //Could have problems
    //    ds.rdd.mapPartitions(rows ⇒ new DGCPTreeMiner().mine(rows.toArray).iterator)
    //    ds.rdd.mapPartitions(rows ⇒ new TopKMiner().mine(rows.toArray).iterator)
    //    ds.rdd.mapPartitions(rows ⇒ new TreeBasedJEPMiner().mine(rows.toArray).iterator)
    //    ds.rdd.mapPartitions(rows ⇒ new SJEPMiner().mine(rows.toArray).iterator)
    //    ds.rdd.mapPartitions {
    //      rows ⇒ {
    //        val miner = new SJEPMiner()
    //        miner.MIN_SUPPORT = 0.2
    //        miner.mine(rows.toArray).iterator
    //      }
    //    }

  }

}
