package miners.pr_spark.fp_max

import org.junit.Assert._
import org.junit.Test

class ItemsetTestFPMax extends FPMaxUtilForTest {

  @Test
  def addItem_true(): Unit = {
    val itemTemp = new Item(2.0, 10.0, nominalFeatureTwo)
    assertTrue(itemset.addItem(itemTemp))
    assertEquals(itemset.items.size, 2)
  }

  @Test
  def addItem_false(): Unit = {
    assertFalse(itemset.addItem(item))
    assertEquals(itemset.items.size, 1)
  }

  @Test
  def addItem_is_ordered_by_feature_index_true(): Unit = {
    val itemTemp = new Item(2.0, 10.0, nominalFeatureTwo)
    val itemsetTemp = new Itemset(Array(itemTemp))
    assertTrue(itemsetTemp.addItem(item))
    assertEquals(itemsetTemp.items.size, 2)
    assertTrue(itemsetTemp.items.head.feature.index < itemsetTemp.items.last.feature.index)
  }

  @Test
  def isSubset_true(): Unit ={
    val itemTemp = new Item(2.0, 10.0, nominalFeatureTwo)
    val itemsetTemp = new Itemset(Array(item))
    assertTrue(itemset.isSubset(itemsetTemp))
    itemsetTemp.addItem(itemTemp)
    assertTrue(itemset.isSubset(itemsetTemp))
  }

  @Test
  def isSubset_false(): Unit ={
    val itemOne = new Item(1.0, 10.0, nominalFeature)
    val itemTwo = new Item(2.0, 10.0, nominalFeatureTwo)
    val itemThree = new Item(3.0, 10.0, nominalFeatureThree)
    itemset.addItem(itemOne)
    val itemsetTemp = new Itemset(Array(itemOne,itemTwo,itemThree))
    assertFalse(itemset.isSubset(itemsetTemp))
  }

  @Test
  def isSuperset_true(): Unit ={
    val itemTemp = new Item(2.0, 10.0, nominalFeatureTwo)
    val itemsetTemp = new Itemset(Array(item))
    assertTrue(itemsetTemp.isSuperset(itemset))
    itemsetTemp.addItem(itemTemp)
    assertTrue(itemsetTemp.isSuperset(itemset))
  }

  @Test
  def isSuperset_false(): Unit ={
    val itemOne = new Item(1.0, 10.0, nominalFeature)
    val itemTwo = new Item(2.0, 10.0, nominalFeatureTwo)
    val itemThree = new Item(3.0, 10.0, nominalFeatureThree)
    itemset.addItem(itemOne)
    val itemsetTemp = new Itemset(Array(itemOne,itemTwo,itemThree))
    assertFalse(itemsetTemp.isSuperset(itemset))
  }

  @Test
  def equal_true(): Unit ={
    val itemOne = new Item(1.0, 10.0, nominalFeatureTwo)
    val itemTwo = new Item(2.0, 10.0, nominalFeatureThree)
    itemset.addItem(itemTwo)
    itemset.addItem(itemOne)
    val itemsetTemp = new Itemset(Array(itemTwo,itemOne, item))
    assertTrue(itemset == itemsetTemp)
  }



}
