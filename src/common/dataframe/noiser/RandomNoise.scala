package common.dataframe.noiser

import common.miner.DatasetSchema
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.udf

import scala.collection.mutable
import scala.util.Random

/**
  * Add noise to the last DataFrame column
  */
object RandomNoise extends Serializable {
  /**
    * Add noise to a DataFrame class column (last column)
    *
    * @param df    DataFrame without noise
    * @param noise Percent of noise
    * @return DataFrame with noise in column class
    */
  def introduceNoise(df: DataFrame, noise: Double): DataFrame = {
    val classFeatures = new DatasetSchema(df.collect()).classFeatures().map(_._1).toList
    val column = classFeatures(0).attribute
    val noiser = udf { clazz: String ⇒
      val others = Random.shuffle(classFeatures.filter(_.value != clazz))
      others(0).value
    }

    val split = df.randomSplit(Array(noise, 1 - noise), 1)
    val modification = split(0).withColumn(column, noiser(df.col(column)))
    split(1).union(modification)
  }

  def introduceNoise(rows: Array[Row], dataMiner: DatasetSchema, noise: Double): Array[Row] = {
    val classFeatures = dataMiner.classFeatures().map(_._1).toList
    val column = rows(0).length - 1
    val schema = rows(0).schema

    val changes = (rows.length * noise).toInt
    val random = new Random()
    val positionToChange = new mutable.HashSet[Int]()
    while (positionToChange.size < changes)
      positionToChange += random.nextInt(rows.length)

    positionToChange.foreach(pos ⇒ {
      val classChange = Random.shuffle(classFeatures.filter(cp ⇒ !cp.isMatch(rows(pos))))
      val values = rows(pos).toSeq.toArray
      values(column) = classChange(0).value
      rows(pos) = new GenericRowWithSchema(values, schema)
    })
    rows
  }
}
