package miners.epm

import java.util

import common.feature._
import common.miner.DatasetSchema
import common.pattern.ContrastPattern
import framework.items.{NominalItem, NumericItem, Pattern}
import keel.Dataset.Attribute
import keel.Dataset.Instance
import keel.Dataset.Attributes
import keel.Dataset.InstanceSet
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._


class SparkToEPM(rows: Array[Row], val dataMiner: DatasetSchema) extends Serializable {

  val instances = new Array[Instance](rows.length)
  val attributes = new Array[Attribute](dataMiner.columnsCount)
  val instanceSet = new InstanceSet()

  makeAttributes(dataMiner)
  makeInstances(rows)
  attributes.foreach(attr ⇒ instanceSet.addAttribute(attr))
  instances.foreach(inst ⇒ instanceSet.addInstance(inst))
  instanceSet.setHeader("Dataset \n\rDataset \n\rDataset \n\rDataset \n\r")


  def makeAttributes(dataMiner: DatasetSchema): Unit = {
    val classIndex = dataMiner.columnsCount - 1
    val input = new util.Vector[String](classIndex)
    val output = new util.Vector[String](1)

    for (i ← dataMiner.columnsInfo.indices) {
      val isClass = i == classIndex
      val field = dataMiner.columnsInfo(i)
      val attribute = field.dataType match {
        case StringType ⇒ makeNominalAttribute(field.name, dataMiner.valuesWithCount(i), isClass)
        case IntegerType ⇒ makeIntegerAttribute(field.name, dataMiner.valuesWithCount(i), isClass)
        case DoubleType ⇒ makeRealAttribute(field.name, dataMiner.valuesWithCount(i), isClass)
        case _ ⇒ throw new IllegalArgumentException("Transformation don't support this type of data")
      }
      if (isClass)
        output.addElement(attribute.getName)
      else input.addElement(attribute.getName)
      attributes(i) = attribute
      Attributes.addAttribute(attribute)
    }
    Attributes.setOutputInputAttributes(input, output)
  }

  private def makeNominalAttribute(name: String, valuesCount: Array[(String, Int)], isClass: Boolean): Attribute = {
    val attr = new Attribute()
    attr.setName(name)
    attr.setType(Attribute.NOMINAL)
    if (isClass) attr.setDirectionAttribute(Attribute.OUTPUT)
    else attr.setDirectionAttribute(Attribute.INPUT)
    attr.setFixedBounds(true)

    valuesCount.foreach(value ⇒ if (value._1 != null) attr.addNominalValue(value._1))
    attr
  }

  private def makeIntegerAttribute(name: String, valuesCount: Array[(String, Int)], isClass: Boolean): Attribute = {
    val attr = new Attribute()
    attr.setName(name)
    attr.setType(Attribute.INTEGER)
    if (isClass) attr.setDirectionAttribute(Attribute.OUTPUT)
    else attr.setDirectionAttribute(Attribute.INPUT)
    val values = valuesCount.map(pair ⇒ pair._1.toDouble.toInt)
    attr.setBounds(values.min, values.max)
    attr
  }

  private def makeRealAttribute(name: String, valuesCount: Array[(String, Int)], isClass: Boolean): Attribute = {
    val attr = new Attribute()
    attr.setName(name)
    attr.setType(Attribute.REAL)
    if (isClass) attr.setDirectionAttribute(Attribute.OUTPUT)
    else attr.setDirectionAttribute(Attribute.INPUT)
    val values = valuesCount.map(pair ⇒ pair._1.toDouble)
    attr.setBounds(values.min, values.max)
    attr
  }

  def makeInstances(rows: Array[Row]): Unit = {
    for (i ← rows.indices) {
      val values = rows(i).mkString(",")
      val instance = new Instance(values, true, i)
      instances(i) = instance
    }
  }

  def patternToContrastPattern(patterns: Array[Pattern]): Array[ContrastPattern] = {
    val contrastPatterns = new Array[ContrastPattern](patterns.length)
    for (i ← patterns.indices) {
      val pattern = patterns(i)
      val items = pattern.getItems.asScala.toArray
      val predicate: Array[Feature[_]] = items.map {
        case it: NominalItem ⇒ nominalItemToCategorical(it)
        case it: NumericItem ⇒ numericItemToNumeric(it)
        case _ ⇒ throw new IllegalArgumentException("Item transform don't support FuzzyItem")
      }
      val clazz = classValueToCategoricalClass(pattern.getClase)
      contrastPatterns(i) = new ContrastPattern(predicate, clazz)
    }
    Attributes.clearAll()
    contrastPatterns
  }

  private def nominalItemToCategorical(item: NominalItem): CategoricalFeature = {
    val index = attributes.indexOf(attributes.find(attr ⇒ attr.getName.equalsIgnoreCase(item.getVariable)).get)
    new CategoricalFeature(item.getVariable, index, item.getValue)
  }

  private def numericItemToNumeric(item: NumericItem): NumericFeature = {
    val index = attributes.indexOf(attributes.find(attr ⇒ attr.getName.equalsIgnoreCase(item.getVariable)).get)
    dataMiner.columnsInfo(index).dataType match {
      case IntegerType ⇒ numericItemToInteger(item, index)
      case DoubleType ⇒ numericItemToDouble(item, index)
    }
  }

  private def numericItemToInteger(item: NumericItem, index: Int): IntegerFeature = {
    item.getOperator match {
      case " = " ⇒ new IntegerFeature(item.getVariable, index, item.getValue) with EqualThan
      case " > " ⇒ new IntegerFeature(item.getVariable, index, item.getValue) with GreaterThan
      case " <= " ⇒ new IntegerFeature(item.getVariable, index, item.getValue) with LessEqualThan
      case " != " ⇒ new IntegerFeature(item.getVariable, index, item.getValue, false) with EqualThan
    }
  }

  private def numericItemToDouble(item: NumericItem, index: Int): DoubleFeature = {
    item.getOperator match {
      case " = " ⇒ new DoubleFeature(item.getVariable, index, item.getValue) with EqualThan
      case " > " ⇒ new DoubleFeature(item.getVariable, index, item.getValue) with GreaterThan
      case " <= " ⇒ new DoubleFeature(item.getVariable, index, item.getValue) with LessEqualThan
      case " != " ⇒ new DoubleFeature(item.getVariable, index, item.getValue, false) with EqualThan
    }
  }

  private def classValueToCategoricalClass(value: Int) = {
    val index = attributes.length - 1
    val attribute = Attributes.getOutputAttribute(0).getName
    val classValue = Attributes.getOutputAttribute(0).getNominalValue(value)
    new CategoricalFeature(attribute, index, classValue)
  }

}
