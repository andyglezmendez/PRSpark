package miners.pr_framework

import java.util

import PRFramework.Core.Common
import PRFramework.Core.Common.Helpers.ArrayHelper
import PRFramework.Core.Common.{FeatureValue, NominalFeature}
import PRFramework.Core.DatasetInfo.{DatasetInformation, NominalFeatureInformation, NumericFeatureInformation}
import PRFramework.Core.SupervisedClassifiers.EmergingPatterns.{EmergingPattern, _}
import common.feature.{CategoricalFeature, DoubleFeature, EqualThan, Feature, GreaterThan, IntegerFeature, _}
import common.miner.DatasetSchema
import common.pattern.ContrastPattern
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField}

import scala.collection.JavaConverters._

class SparkToPRFramework private() extends Serializable {

  var model: Common.InstanceModel = _
  var instances: util.ArrayList[Common.Instance] = _
  var classFeature: Common.Feature = _

  def this(rows: Array[Row], dataMiner: DatasetSchema) {
    this()
    this.model = makeInstanceModel(dataMiner)
    this.instances = makePrInstance(rows)
    this.classFeature = model.getFeatures.last
  }

  /**
    * Construct an InstanceModel using the MinerInformation
    *
    * @param dataMiner MinerInformation
    * @return InstanceModel used by PRFramework
    */
  def makeInstanceModel(dataMiner: DatasetSchema): Common.InstanceModel = {
    val instanceModel = new Common.InstanceModel
    instanceModel.setRelationName("spark-dataset")
    instanceModel.setFeatures(makePrFeatures(dataMiner))
    instanceModel.setDatasetInformation(new DatasetInformation {
      setGlobalAbscenseInformation(0)
      setObjectsWithIncompleteData(0)
      setFeatureInformations(instanceModel.getFeatures.map(f ⇒ f.getFeatureInformation))
    })

    instanceModel
  }

  /**
    * Transform every Row in rows to an Instance used in PRFramework
    *
    * @param rows Array of Row. Instance from the dataset
    * @return Array of Instance
    */
  def makePrInstance(rows: Array[Row]): util.ArrayList[Common.Instance] = {
    val instances = new util.ArrayList[Common.Instance](rows.length)
    for (row ← rows) {
      val instance = new Common.Instance(model)
      //      val instance = model.CreateInstance()

      for (i ← 0 until row.size) {
        model.getFeature(i) match {
          case f: Common.NominalFeature ⇒ {
            if (row.isNullAt(i)) instance.set(f, FeatureValue.Missing)
            else instance.SetNominalValue(f, row.getString(i))
          }
          case f: Common.IntegerFeature ⇒ {
            if (row.isNullAt(i)) instance.set(f, FeatureValue.Missing)
            else instance.set(f, row.getInt(i))
          }
          case f: Common.DoubleFeature ⇒ {
            if (row.isNullAt(i)) instance.set(f, FeatureValue.Missing)
            else instance.set(f, row.getDouble(i))
          }
        }
      }
      instances.add(instance)
    }
    instances
  }

  def makePrFeatures(dataMiner: DatasetSchema): Array[Common.Feature] = {
    val fields = dataMiner.columnsInfo

    val featureCount = fields.length
    val features = new Array[Common.Feature](featureCount)

    for (i ← 0 until featureCount) {
      fields(i).dataType match {
        case StringType ⇒ features(i) = makeNominalFeature(fields(i), i, dataMiner.valuesWithCount(i))
        case IntegerType ⇒ features(i) = makeIntegerFeature(fields(i), i, dataMiner.valuesWithCount(i))
        case DoubleType ⇒ features(i) = makeDoubleFeature(fields(i), i, dataMiner.valuesWithCount(i))
      }
    }
    features
  }

  private def makeNominalFeature(field: StructField, index: Int, values: Array[(String, Int)]): Common.NominalFeature = {
    val nominalFeature = new Common.NominalFeature(field.name, index)

    val notNullValues = values.filter(_._1 != null)
    nominalFeature.setValues(notNullValues.map(_._1))

    val nullValue = values.find(_._1 == null).orNull
    var nullValuesCount = 0
    if(nullValue != null)
      nullValuesCount = nullValue._2

    nominalFeature.setFeatureInformation(new NominalFeatureInformation {
      setFeature(nominalFeature)
      setMissingValueCount(nullValuesCount)
      setDistribution(notNullValues.map(_._2.toDouble))
      setValueProbability(getDistribution.map(d ⇒ d / getDistribution.sum))
      setRatio(getDistribution.map(d ⇒ d / getDistribution.min))
    })

    nominalFeature
  }

  private def makeIntegerFeature(field: StructField, index: Int, values: Array[(String, Int)]): Common.IntegerFeature = {
    val integerFeature = new Common.IntegerFeature(field.name, index)

    val notNullValues = values.filter(_._1 != null)
    val valuesInteger = notNullValues.map(_._1.toDouble)
    integerFeature.setMaxValue(valuesInteger.max)
    integerFeature.setMinValue(valuesInteger.min)

    val nullValue = values.find(_._1 == null).orNull
    var nullValuesCount = 0
    if(nullValue != null)
      nullValuesCount = nullValue._2

    integerFeature.setFeatureInformation(new NumericFeatureInformation {
      setFeature(integerFeature)
      setMissingValueCount(nullValuesCount)
      MinValue = integerFeature.getMinValue
      MaxValue = integerFeature.getMaxValue
    })

    integerFeature
  }

  private def makeDoubleFeature(field: StructField, index: Int, values: Array[(String, Int)]): Common.DoubleFeature = {
    val doubleFeature = new Common.DoubleFeature(field.name, index)

    val notNullValues = values.filter(_._1 != null)
    val valuesDouble = notNullValues.map(_._1.toDouble)
    doubleFeature.setMaxValue(valuesDouble.max)
    doubleFeature.setMinValue(valuesDouble.min)

    val nullValue = values.find(_._1 == null).orNull
    var nullValuesCount = 0
    if(nullValue != null)
      nullValuesCount = nullValue._2

    doubleFeature.setFeatureInformation(new NumericFeatureInformation {
      setFeature(doubleFeature)
      setMissingValueCount(nullValuesCount)
      MinValue = doubleFeature.getMinValue
      MaxValue = doubleFeature.getMaxValue
    })

    doubleFeature
  }

  def emergingPatternToContrastPattern(patterns: Array[IEmergingPattern]): Array[ContrastPattern] = {
    patterns.map(pattern ⇒ {
      val items: Array[Feature[_]] = pattern.getItems.asScala.map(item ⇒ itemToSparkFeature(item)).toArray
      val clas = featureToSparkFeature(pattern.getClassFeature, pattern.getClassValue)
      val conPattern = new ContrastPattern(items, clas)
      val classPattern = pattern.asInstanceOf[EmergingPattern]
      conPattern.pp = classPattern.getContingence.apply(0).toInt
      conPattern.pn = classPattern.getContingence.apply(1).toInt
      conPattern.np = classPattern.getContingence.apply(2).toInt
      conPattern.nn = classPattern.getContingence.apply(3).toInt
      conPattern
    })
  }


  //--------------ITEM TO FEATURE WHEN TRANSFORM EMERGINGPATTERN IN CONTRASTPATTERN-------------------------------

  /**
    * Is used to transform an EmergingPattern from PRFramework in ContrastPattern
    * Transform an PRFramework item in the equal Feature
    *
    * @param item PRFramework.Core.SupervisedClassifiers.EmergingPatterns.Item
    */
  def itemToSparkFeature(item: Item): common.feature.Feature[_] = {
    item.getFeature match {
      case feature: Common.CategoricalFeature ⇒ categoricalFeatureToCategorical(item, feature)
      case feature: Common.IntegerFeature ⇒ integerFeatureToInteger(item, feature)
      case feature: Common.DoubleFeature ⇒ doubleFeatureToDouble(item, feature)
      case _ ⇒ throw new IllegalArgumentException("I can't manage this Feature")
    }
  }

  private def categoricalFeatureToCategorical(item: Item, feature: Common.CategoricalFeature) = {
    item match {
      case it: EqualThanItem ⇒
        new CategoricalFeature(feature.getName, feature.getIndex, feature.Values(it.getValue.toInt))
      case it: DifferentThanItem ⇒
        new CategoricalFeature(feature.getName, feature.getIndex, feature.Values(it.getValue.toInt), true)
    }
  }

  private def integerFeatureToInteger(item: Item, feature: Common.IntegerFeature) = {
    item match {
      case it: EqualThanItem ⇒ new IntegerFeature(feature.getName, feature.getIndex, it.getValue) with EqualThan
      case it: DifferentThanItem ⇒ new IntegerFeature(feature.getName, feature.getIndex, it.getValue, true) with EqualThan
      case it: GreatherThanItem ⇒ new IntegerFeature(feature.getName, feature.getIndex, it.getValue) with GreaterThan
      case it: LessOrEqualThanItem ⇒ new IntegerFeature(feature.getName, feature.getIndex, it.getValue) with LessEqualThan
      case _ ⇒ throw new IllegalArgumentException("The PRFramework item is not defined")
    }
  }

  private def doubleFeatureToDouble(item: Item, feature: Common.DoubleFeature) = {
    item match {
      case it: EqualThanItem ⇒ new DoubleFeature(feature.getName, feature.getIndex, it.getValue) with EqualThan
      case it: DifferentThanItem ⇒ new DoubleFeature(feature.getName, feature.getIndex, it.getValue, true) with EqualThan
      case it: GreatherThanItem ⇒ new DoubleFeature(feature.getName, feature.getIndex, it.getValue) with GreaterThan
      case it: LessOrEqualThanItem ⇒ new DoubleFeature(feature.getName, feature.getIndex, it.getValue) with LessEqualThan
      case _ ⇒ throw new IllegalArgumentException("The PRFramework item is not defined")
    }
  }

  private def featureToSparkFeature(feature: Common.Feature, value: Double): CategoricalFeature = {
    feature match {
      case f: Common.CategoricalFeature ⇒ new CategoricalFeature(f.getName, f.getIndex, f.Values(value.toInt))
      case _ ⇒ throw new IllegalArgumentException("I can't manage this feature")
    }
  }

}

