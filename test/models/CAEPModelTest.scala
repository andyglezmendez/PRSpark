package models

import common.dataframe.dicretizer.{EntropyBasedDiscretizer, IntegerToStringDiscretizer}
import common.serialization.{ARFFSerializer, DirectoryTool}
import common.utils.ClassSelector
import miners.pr_framework.miner.RandomForestMiner
import org.apache.hadoop.mapred.FileAlreadyExistsException
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.junit.Test

class CAEPModelTest {



  @Test
  def prueba(): Unit ={
    val conf = new SparkConf().setMaster(s"local[*]")
      .setAppName("PR-Spark-DB-Process")
      .set("spark.sql.warehouse.dir", "..\\")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = SparkContext.getOrCreate()

    val serializer = new ARFFSerializer()
    var df = serializer.loadDataFrameFromARFF("D:\\Home\\School\\Tesis\\BD\\zoo.arff")
    df = new IntegerToStringDiscretizer().discretize(df)

    val instances = df.randomSplit(Array(0.7,0.3))
    val training = instances(0).collect()
    val test = instances(1).collect()

    val miner = new RandomForestMiner()
    val patterns = miner.mine(training)

    val caep = new CAEPModel()
    caep.initialize(patterns, training, miner.dataMiner)

    var good = 0
    for(instance ← test){
      val prediction = caep.predict(instance)
      if(caep.getPredictionClass(prediction).isMatch(instance))
        good += 1
    }

    println(s"GOOD => ${good}")
    println(s"BAD => ${test.length - good}")

  }

}
