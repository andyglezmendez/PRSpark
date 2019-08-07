package miners.pr_spark.fp_max

import PRFramework.Core.Common.NominalFeature
import common.feature.CategoricalFeature
import org.junit.{Before, Test}
import org.junit.Assert._

import scala.collection.{SortedMap, mutable}

class FPMaxTest {

  var features: Array[CategoricalFeature] = _
  var items: Array[Item] = _
  var transactions: Array[Itemset] = _

  @Before
  def init(): Unit = {
    initFeature()
    initItems()
    initTransactions()
  }

  def initTransactions(): Unit = {
    val itemset0 = new Itemset(Array(items(0), items(3), items(7)))
    val itemset1 = new Itemset(Array(items(2), items(5), items(6)))
    val itemset2 = new Itemset(Array(items(0), items(5), items(6)))
    val itemset3 = new Itemset(Array(items(2), items(3), items(8)))

    val itemset4 = new Itemset(Array(items(1), items(4), items(8)))
    val itemset5 = new Itemset(Array(items(0), items(3), items(8)))
    val itemset6 = new Itemset(Array(items(0), items(4), items(8)))

    transactions = Array(itemset0, itemset1, itemset2, itemset3, itemset4, itemset5, itemset6)
  }

  def initItems(): Unit = {
    val item0 = new Item(0.0, 4, features(0))
    val item1 = new Item(1.0, 1, features(0))
    val item2 = new Item(2.0, 2, features(0))
    val item3 = new Item(0.0, 2, features(1))
    val item4 = new Item(1.0, 3, features(1))
    val item5 = new Item(2.0, 2, features(1))
    val item6 = new Item(0.0, 2, features(2))
    val item7 = new Item(1.0, 1, features(2))
    val item8 = new Item(2.0, 4, features(2))
    val item9 = new Item(0.0, 4, features(3))
    val item10 = new Item(1.0, 3, features(3))

    items = Array(item0, item1, item2, item3, item4, item5, item6, item7, item8, item9, item10)
  }

  def initFeature(): Unit = {
    val feature0 = new CategoricalFeature("One", 0,"")

    val feature1 = new CategoricalFeature("Two", 1, "")

    val feature2 = new CategoricalFeature("Three", 2, "")

    val featureClass = new CategoricalFeature("Class", 2, "")

    features = Array(feature0, feature1, feature2, featureClass)
  }

  @Test
  def fp_max_test(): Unit = {
    val extractor1 = new FPMaxItemsetExtractor()
    val extractor2 = new FPMaxItemsetExtractor()

    var freq1 = Array(items(0), items(2), items(3), items(5), items(6), items(7), items(8))
    var freq2 = Array(items(0), items(1), items(3), items(4), items(8))
    val itemsets1 = transactions.slice(0, 4)
    val itemsets2 = transactions.slice(4, 7)

    val patterns1 = extractor1.extractMaximaItemsets(freq1, itemsets1, 1)
    val patterns2 = extractor2.extractMaximaItemsets(freq2, itemsets2, 1)

    println("Class A")
    patterns1.foreach(itemset ⇒ {
      itemset.items.foreach(item ⇒ print(s"$item, "))
      println("")
    })
    println();println("Class B")
    patterns2.foreach(itemset ⇒ {
      itemset.items.foreach(item ⇒ print(s"$item, "))
      println("")
    })

    assertEquals(4, patterns1.size)
    assertEquals(3, patterns2.size)

    for (pattern ← patterns1) {
      assertEquals(3, pattern.items.size)
    }
    for (pattern ← patterns2) {
      assertEquals(3, pattern.items.size)
    }
  }

}
