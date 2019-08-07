package experiments

import java.nio.file.{Files, Paths}

import common.dataframe.dicretizer.{EntropyBasedDiscretizer, IntegerToStringDiscretizer}
import common.filter.JEPFilter
import common.miner.DatasetSchema
import common.pattern.ContrastPattern
import common.serialization.{ARFFSerializer, DirectoryTool}
import common.utils.ClassSelector
import experiment.accuracy.PararellAccuracyExperiment
import experiment.jep.PararellJEPExperiment
import experiment.minimal.PararellMinimalExperiment
import miners.epm.miner._
import miners.pr_framework.miner.{CEPMMiner, LCMiner, RandomForestMiner}
import models.{BCEPModel, CAEPModel, DTMModel, iCAEPModel}
import org.apache.hadoop.mapred.FileAlreadyExistsException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

class TestALL {

  @Test
  def unionRDD(): Unit = {

    val exp = new PararellJEPExperiment()
    val noise = Array(0.05, 0.08, 0.12, 0.15, 0.18, 0.22, 0.26, 0.30)
    //    exp.makeNoise(noise)
    val patt = "noise-0.12"

    val path = "D:\\Home\\School\\Tesis\\Results\\serialized_patterns\\merged\\NEW"

    val noiseDB = DirectoryTool.loadFiles(path, s"${patt}*").map(DirectoryTool.fileName)

    val conf = new SparkConf().setMaster(s"local[1]")
      .setAppName("PR-Spark-DB-Process")
      .set("spark.sql.warehouse.dir", "..\\")
      .set("spark.executor.memory", "1g")
      .set("spark.driver.memory", "6g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = SparkContext.getOrCreate()

    val patterns = new ArrayBuffer[RDD[(String, Array[(Array[ContrastPattern], Array[Row], DatasetSchema, Array[Row])])]]()
    for (db ← noiseDB) {
      val temp: RDD[(String, Array[(Array[ContrastPattern], Array[Row], DatasetSchema, Array[Row])])] =
        sc.objectFile(s"${path}\\${db}")
      //      temp.collect().foreach(println)
      patterns += temp
    }


    val join = patterns.reduce((a, b) ⇒ a union b)

    join.saveAsObjectFile(path + "\\" + patt)

  }

  @Test
  def no_jeps(): Unit = {

    val exp = new PararellJEPExperiment()
    val noise = Array(0.05, 0.08, 0.12, 0.15, 0.18, 0.22, 0.26, 0.30)
    //    exp.makeNoise(noise)
    val noiseDB = DirectoryTool.loadFiles(exp.noiseDB, "*noise*").map(DirectoryTool.fileName)
    val size = 200
    val split = 20


    for (db ← noiseDB) {
      var from = 98
      var end = from + split - 1
      while (from < size - 1) {
        exp.mineNoiseDB(db, db + s"[${from}-${end}]", from, end)
        from = end + 1
        end = from + split - 1
        if (end >= size) end = size - 1
      }
    }


    //        exp.classifyNoisesDB("no-jep", Array(new BCEPModel, new iCAEPModel), noiseDB)
    //        exp.toTabularVziew()

  }

  @Test
  def accuracy(): Unit = {
    val exp = new PararellJEPExperiment()
    exp.discretize = true
    //    exp.serializeDB()
    //    exp.prepareCrossValidation()
    val dbs = DirectoryTool.loadFiles(exp.processedDB + "\\discretized", "all").map(DirectoryTool.fileName)
//            exp.mineDB("all", "all")

//    val support = Array(0.02, 0.06, 0.10, 0.15, 0.18, 0.20, 0.25, 0.30)

    //    support.map(supp ⇒ exp.classifyNoisesDB("accuracy", Array(new BCEPModel, new iCAEPModel), Array("all"), supp))
    //    exp.toTabularView()
  }

  @Test
  def solo_minimales(): Unit = {
    val exp = new PararellMinimalExperiment()
    exp.discretize = true
    //    exp.serializeDB()
    //    exp.prepareCrossValidation()
    //    val dbs = DirectoryTool.loadFiles(exp.processedDB + "\\discretized", "all").map(DirectoryTool.fileName)
    //        exp.mineDB("all", "all")

    val support = Array(0.02, 0.06, 0.10, 0.15, 0.18, 0.20, 0.25, 0.30)

    support.map(supp ⇒ exp.classify("minimal", Array(new BCEPModel, new iCAEPModel), Array("all"), supp))
    exp.toTabularView()
  }

  @Test
  def no_minimals(): Unit = {
    val exp = new PararellMinimalExperiment()
    exp.discretize = true
    //    exp.serializeDB()
    //    exp.prepareCrossValidation()
    //    val dbs = DirectoryTool.loadFiles(exp.processedDB + "\\discretized", "all").map(DirectoryTool.fileName)
    //            exp.mineDB("all", "all")

    val support = Array(0.02, 0.06, 0.10, 0.15, 0.18, 0.20, 0.25, 0.30)

    //        support.map(supp ⇒ exp.classify("no-minimal", Array(new iCAEPModel), Array("all"), supp))
    exp.toTabularView()
  }

  @Test
  def jep(): Unit = {
    val exp = new PararellJEPExperiment()

    //    val patterns = DirectoryTool.loadFiles(exp.processedPatterns, "noise-0.12*").map(DirectoryTool.fileName)
    //    val patterns = DirectoryTool.loadFiles(exp.processedPatterns, "noise-0.15*").map(DirectoryTool.fileName)

    //    exp.classifyNoisesDB("rfm-jep", Array(new BCEPModel), patterns)
    exp.toTabularView()

  }

  @Test
  def acc(): Unit = {
    val exp = new PararellAccuracyExperiment()

    //        val support = Array(0.0)
    val support = Array(0.0, 0.02, 0.06, 0.10, 0.15, 0.18, 0.20, 0.25, 0.30)

    support.map(supp ⇒ exp.classify("acc", Array(new iCAEPModel), Array("all"), supp))
    //    exp.toTabularView()
  }

  @Test
  def minimal(): Unit = {
    val exp = new PararellMinimalExperiment()

    val support = Array(0.02, 0.06, 0.10, 0.15, 0.18, 0.20, 0.25, 0.30)

    support.map(supp ⇒ exp.classify("min", Array(new CAEPModel), Array("all"), supp))
    //    exp.toTabularView()
  }

  @Test
  def icaep(): Unit ={
    val conf = new SparkConf().setMaster(s"local[*]")
      .setAppName("PR-Spark-DB-Process")
      .set("spark.sql.warehouse.dir", "..\\")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = SparkContext.getOrCreate()

    val ser = new ARFFSerializer()
    var df = ser.loadDataFrameFromARFF("resource/db/zoo.arff")
    val disc = new IntegerToStringDiscretizer()
    df = disc.discretize(df)
    val miner = new SJEPMiner()
    miner.MIN_SUPPORT = 0
//    miner.MIN_GROWTH_RATE = 1
//    val miner = new LCMiner() //184

    var patterns = miner.mine(df.collect())
//val a = patterns.count(_.pn > 1)
//    val exp = new PararellMinimalExperiment()

    val support = Array(0.02, 0.06, 0.10, 0.15, 0.18, 0.20, 0.25, 0.30)

//    support.map(supp ⇒ exp.classify("min", Array(new CAEPModel), Array("all"), supp))
    //    exp.toTabularView()
  }
}
