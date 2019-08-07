import java.io.PrintWriter
import java.nio.file.{Files, Paths}
import java.util.regex.Pattern

import common.miner.DatasetSchema
import common.pattern.ContrastPattern
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import scala.io

object Runn {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(s"local[*]")
      .setAppName("PR-Spark-DB-Process")
      .set("spark.sql.warehouse.dir", "..\\")
      .set("spark.executor.memory", "2g")
      .set("spark.driver.memory", "6g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = SparkContext.getOrCreate()
    val dir = "D:\\Home\\School\\Tesis\\Results\\serialized_patterns\\merged\\all"
    val dirToSave = "D:\\Home\\School\\Tesis\\Results\\spark\\new\\DB-DATA.csv"

    //Load serialized databases
    val rddPatterns: RDD[(String, Array[(Array[ContrastPattern], Array[Row], DatasetSchema, Array[Row])])] =
      sc.objectFile(dir)
    var results = rddPatterns.map(tuple ⇒ {
      val db = tuple._1
      val instances = tuple._2(0)._4.length + tuple._2(0)._2.length
      val columns = tuple._2(0)._3.columnsCount
      val items = tuple._2(0)._3.valuesWithCount.map(t ⇒ t.length).sum
      val classCount = tuple._2(0)._3.classFeatures().length
      (db, instances, columns, items, classCount)
    }).collect()
    tabular(results)

  }

  def tabular(results: Array[(String, Int, Int, Int, Int)]) = {

    for (result ← results) {

      val str = new StringBuilder()

      if (!Files.exists(Paths.get("D:\\Home\\School\\Tesis\\Results\\spark\\new\\DB-DATA.csv")))
        str.append("db\tinstances\tcolumns\titems\nclass")
      else {
        val old = io.Source.fromFile("D:\\Home\\School\\Tesis\\Results\\spark\\new\\DB-DATA.csv").mkString
        str.appendAll(old)
      }

      str.append(s"${result._1}\t${result._2}\t${result._3}\t${result._4}\t${result._5}\n")
      val writer = new PrintWriter("D:\\Home\\School\\Tesis\\Results\\spark\\new\\DB-DATA.csv")
      writer.write(str.mkString)
      writer.flush()
      writer.close()
    }
  }

}
