import java.time.LocalTime

import common.feature.{CategoricalFeature, Feature}
import common.miner.DatasetSchema
import common.pattern.ContrastPattern
import experiment.ModelEvaluation
import models.BCEPModel
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.Test

class BCEPMilton extends ModelEvaluation {

  protected var features: Array[Feature[_]] = _
  protected var patterns: Array[ContrastPattern] = _


  @Test
  def best_case(): Unit = {
    val instance = make_instance()
    val patterns = patterns_best_case()
    val dataMiner = new DatasetSchema(Array(instance))
    dataMiner.instanceCount = 100

    val bcep = new BCEPModel
    bcep.initialize(patterns, Array(instance), dataMiner)

    val init = LocalTime.now()
    val prediction = bcep.predict(instance)
    val end = LocalTime.now()
    println(end.getNano - init.getNano)
    println(bcep.getPredictionClass(prediction))

  }

  @Test
  def worst_case(): Unit = {
    val instance = make_instance()
    val patterns = patterns_worst_case()
    val dataMiner = new DatasetSchema(Array(instance))
    dataMiner.instanceCount = 100


    val bcep = new BCEPModel
    bcep.initialize(patterns, Array(instance), dataMiner)

    val init = LocalTime.now()
    val prediction = bcep.predict(instance)
    val end = LocalTime.now()
    println(end.getNano - init.getNano)
    println(bcep.getPredictionClass(prediction))
  }

  def load_data(): DataFrame = {
    val conf = new SparkConf().setMaster("local")
      .setAppName("My App")
      .set("spark.sql.warehouse.dir", "..\\")
    val ss = SparkSession.builder().config(conf).getOrCreate()
    ss.read.option("header", "true").csv("resource\\matrix.csv")
  }

  def make_instance(): Row = {
    val f1 = new StructField("Col_1", DataTypes.StringType)
    val f2 = new StructField("Col_2", DataTypes.StringType)
    val f3 = new StructField("Col_3", DataTypes.StringType)
    val f4 = new StructField("Col_4", DataTypes.StringType)
    val f5 = new StructField("Col_5", DataTypes.StringType)
    val f6 = new StructField("Col_6", DataTypes.StringType)
    val c7 = new StructField("Class", DataTypes.StringType)
    val struct = new StructType(Array(f1, f2, f3, f4, f5, f6, c7))

    new GenericRowWithSchema(Array("A", "B", "C", "1", "2", "3", "C_1"), struct)
  }

  def patterns_best_case(): Array[ContrastPattern] = {
    initPatternFeatures()
    Array(
      new ContrastPattern(features.take(3), features.last),
      new ContrastPattern(Array(features(3), features(4), features(5)), features.last)
    )
  }

  def patterns_worst_case(): Array[ContrastPattern] = {
    initPatternFeatures()
    Array(
      new ContrastPattern(Array(features(0)), features.last),
      new ContrastPattern(Array(features(0),features(1)), features.last),
      new ContrastPattern(Array(features(0),features(1),features(2)), features.last),
      new ContrastPattern(Array(features(0),features(1),features(2),features(3)), features.last),
      new ContrastPattern(Array(features(1), features(2), features(3), features(4)), features.last),
      new ContrastPattern(Array(features(2), features(3), features(4), features(5)), features.last)
    )
  }

  private def initPatternFeatures(): Unit = {
    features = Array(
      new CategoricalFeature("Col_1", 0, "A"), new CategoricalFeature("Col_2", 1, "B"),
      new CategoricalFeature("Col_3", 2, "C"), new CategoricalFeature("Col_4", 3, "1"),
      new CategoricalFeature("Col_5", 4, "2"), new CategoricalFeature("Col_6", 5, "3"),
      new CategoricalFeature("Class", 6, "C_1")
    )
  }

}
