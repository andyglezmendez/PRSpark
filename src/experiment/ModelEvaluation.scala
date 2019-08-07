package experiment

import common.serialization.ARFFSerializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

trait ModelEvaluation {
  var spark: SparkSession = _
  var sql: SQLContext = _
  var dbName: String = _

  def initializeSpark() = {
    val conf = new SparkConf().setMaster("local")
      .setAppName("PR-Spark-Testing")
      .set("spark.sql.warehouse.dir", "..\\")

    spark = SparkSession.builder().config(conf).getOrCreate()
    sql = spark.sqlContext
  }

  def loadDataFrame(path: String): DataFrame = {
    val serializer = new ARFFSerializer()
    serializer.loadDataFrameFromARFF(path)
  }

  def loadRows(path: String): Array[Row] = {
    val serializer = new ARFFSerializer()
    serializer.loadRowsFromARFF(path)
  }

  def rowsToDataFrame(rows: Array[Row]): DataFrame ={
    val rdd = sql.sparkContext.parallelize(rows)
    val df = spark.createDataFrame(rdd, rdd.first().schema)
    df.cache()
    df
  }

}
