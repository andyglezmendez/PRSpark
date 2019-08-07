package miners.pr_spark.fp_max

import PRFramework.Core.Common.NominalFeature
import common.feature.CategoricalFeature
import org.junit.Assert._
import org.junit.Test

class ForTestFPMax extends FPMaxUtilForTest {

  @Test
  def item_equal_true(): Unit ={
    val itemTemp = new Item(item.value, item.support, item.feature)
    assertTrue(item == itemTemp)
  }

  @Test
  def item_equal_for_feture_false(): Unit ={
    val feature = new CategoricalFeature("Feature1", 0,"")

    val itemTemp = new Item(item.value, item.support, feature)
    assertFalse(item == itemTemp)
  }

  @Test
  def item_equal_for_value_false(): Unit ={
    val itemTemp = new Item(2.0, item.support, item.feature)
    assertFalse(item == itemTemp)
  }

  @Test
  def item_equal_for_support_false(): Unit ={
    val itemTemp = new Item(item.value, 1.1, item.feature)
    assertFalse(item == itemTemp)
  }

}
