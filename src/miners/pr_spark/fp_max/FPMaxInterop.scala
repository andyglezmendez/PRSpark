package miners.pr_spark.fp_max

import common.feature.{CategoricalFeature, Feature}
import common.miner.DatasetSchema
import common.pattern.ContrastPattern
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

private[fp_max] class FPMaxInterop(rows: Array[Row], dataMiner: DatasetSchema, minimalSuppor: Double) extends Serializable {

  var itemsByDistribution = new Array[mutable.HashSet[Item]](dataMiner.columnsCount)
  var minSupportByDistribution = new Array[Int](dataMiner.columnsCount)
  var transactions = new Array[ArrayBuffer[Itemset]](dataMiner.valuesWithCount(dataMiner.columnsCount - 1).length)

  private val supports = new Array[Array[Int]](transactions.length)
  private var frequentItems = new ListBuffer[Item]
  private val classSparkFeature = extractClassFeature(dataMiner)

  extractFrequenItems(dataMiner)
  extractTransactions(rows, dataMiner)

  def extractFrequenItems(dataMiner: DatasetSchema): Unit = {
    val classDistribution = dataMiner.valuesWithCount(dataMiner.columnsCount - 1).map(_._2)
    for (i ← classDistribution.indices) {
      minSupportByDistribution(i) = math.ceil(minimalSuppor * classDistribution(i)).toInt
    }
    val minCount = minSupportByDistribution.min

    val features = extractFeatures(dataMiner)
    for (feature ← features) {
      if (dataMiner.columnsInfo(feature.index).dataType.isInstanceOf[StringType]) {
        val valueCount = dataMiner.valuesWithCount(feature.index).find(pair ⇒ pair._1.equals(feature.value)).orNull
        val value = dataMiner.valuesWithCount(feature.index).indexOf(valueCount)
        frequentItems += new Item(value, valueCount._2, feature)
      }
    }
  }

  def extractTransactions(rows: Array[Row], dataMiner: DatasetSchema): Unit = {
    for (i ← supports.indices) {
      supports(i) = new Array[Int](frequentItems.size)
    }
    for (i ← transactions.indices) {
      transactions(i) = new ArrayBuffer[Itemset]
    }

    for (row ← rows) {
      var transaction = new Itemset()
      val classValue = dataMiner.valuesWithCount(dataMiner.columnsCount - 1).map(_._1).indexOf(row.getString(dataMiner.columnsCount - 1))

      for (i ← 0 until row.size) {
        if (i == dataMiner.columnsCount - 1) {
          val positionValue = dataMiner.valuesWithCount(i).map(_._1).indexOf(row.getString(i))
          transactions(positionValue) += transaction
          //continue
        }
        else {

          var seekItem = frequentItems.find(item ⇒ {
            if (item.feature.index == i && dataMiner.valuesWithCount(i)(item.value.toInt)._1 == row.getString(i)) {
              supports(classValue)(frequentItems.indexOf(item)) += 1
              true
            }
            else false
          }).orNull

          if (seekItem != null) transaction.items += seekItem
        }
      }
    }
    //Clean transactions with frequent items accord to distribution
    for (i ← transactions.indices) {
      var frequentItemsD = new mutable.HashSet[Item]()

      for (transaction ← transactions(i)) {
        transaction.items = transaction.items.filter(item ⇒ {

          var num = 0
          var seekItem = frequentItems.find(it ⇒ {
            if (item == it && supports(i)(frequentItems.indexOf(it)) >= minSupportByDistribution(i)) {
              num = frequentItems.indexOf(it)
              true
            }
            else false
          }).orNull

          if (seekItem != null) {
            seekItem.support = supports(i)(num)
            frequentItemsD += seekItem
            true
          }
          else false
        })
      }
      itemsByDistribution(i) = frequentItemsD
    }
  }

  def extractContrastPatterns(itemsets: Array[Itemset], classValue: Int): Array[ContrastPattern] = {
    val itemsMap = frequentItems.map(item ⇒ {
      val value = item.feature.value
      val sparkFeature = new CategoricalFeature(item.feature.attribute, item.feature.index, value)
      (item, sparkFeature)
    }).toMap

    val patterns = itemsets.map(itemset ⇒ {
      val sparkFeatures: Array[Feature[_]] = itemset.items.map(item ⇒ itemsMap.get(item).orNull).toArray
      new ContrastPattern(predicate = sparkFeatures, clazz = classSparkFeature(classValue))
    })
    patterns
  }

  private def extractClassFeature(dataMiner: DatasetSchema): Array[CategoricalFeature] = {
    val classPosition = dataMiner.columnsCount - 1
    val columnClass = dataMiner.columnsInfo(classPosition)
    val columnClassValues = dataMiner.valuesWithCount(classPosition).map(_._1)
    columnClassValues.map(value ⇒ new CategoricalFeature(columnClass.name, classPosition, value))
  }

  private def extractFeatures(dataMiner: DatasetSchema): ArrayBuffer[CategoricalFeature] = {
    val features = new ArrayBuffer[CategoricalFeature]()
    for (i ← dataMiner.valuesWithCount.indices) {
      if (dataMiner.columnsInfo(i).dataType == StringType) {
        val attribute = dataMiner.columnsInfo(i).name
        features ++= dataMiner.valuesWithCount(i).map(pair ⇒ new CategoricalFeature(attribute, i, pair._1))
      }
    }
    features
  }
}
