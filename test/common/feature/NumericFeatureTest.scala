package common.feature

import common.CommonUtilForTest
import org.junit.Assert._
import org.junit.Test

class NumericFeatureTest extends CommonUtilForTest {

  @Test
  def equal_than_true(): Unit = {
    val feature = new IntegerFeature("A", 0, 1) with EqualThan
    assertTrue(feature == integerFeatures(0))
  }

  @Test
  def equal_than_false(): Unit = {
    val feature = new IntegerFeature("A", 0, 2) with EqualThan
    assertFalse(feature == integerFeatures(0))
    assertFalse(feature == integerFeatures(1))
  }

  @Test
  def greater_than_supergroup_true(): Unit = {
    val f1: GreaterThan = integerFeatures(1).asInstanceOf[GreaterThan]
    val f2 = integerFeatures(2).asInstanceOf[GreaterThan]
    val f3 = new IntegerFeature("A", 0, 2) with GreaterThan

    assertTrue(f1 isSupergroupOf f2)
    assertTrue(f1 isSupergroupOf f1) // Is self supergroup
  }

  @Test
  def greater_than_supergroup_false(): Unit = {
    val f1: GreaterThan = integerFeatures(1).asInstanceOf[GreaterThan]
    val f2 = integerFeatures(2).asInstanceOf[GreaterThan]

    assertFalse(f2 isSupergroupOf f1)
  }

  @Test
  def greater_than_subgroup_true(): Unit = {
    val f1: GreaterThan = integerFeatures(1).asInstanceOf[GreaterThan]
    val f2 = integerFeatures(2).asInstanceOf[GreaterThan]
    val f3 = new IntegerFeature("A", 0, 2) with GreaterThan

    assertTrue(f2 isSubgroupOf f1)
  }

  @Test
  def greater_than_subgroup_false(): Unit = {
    val f1: GreaterThan = integerFeatures(1).asInstanceOf[GreaterThan]
    val f2 = integerFeatures(2).asInstanceOf[GreaterThan]
    val f3 = new IntegerFeature("A", 0, 2) with GreaterThan

    assertFalse(f1 isSubgroupOf f2)
    assertFalse(f1 isSubgroupOf f1) // Is not self subgroup
  }


  @Test
  def less_than_supergroup_true(): Unit = {
    val f1: LessThan = integerFeatures(4).asInstanceOf[LessThan]
    val f2 = integerFeatures(5).asInstanceOf[LessThan]
    val f3 = new IntegerFeature("A", 0, 2) with LessThan

    assertTrue(f2 isSupergroupOf f1)
    assertTrue(f1 isSupergroupOf f1) // Is self supergroup
  }

  @Test
  def less_than_supergroup_false(): Unit = {
    val f1: LessThan = integerFeatures(4).asInstanceOf[LessThan]
    val f2 = integerFeatures(5).asInstanceOf[LessThan]

    assertFalse(f1 isSupergroupOf f2)
  }

  @Test
  def less_than_subgroup_true(): Unit = {
    val f1: LessThan = integerFeatures(4).asInstanceOf[LessThan]
    val f2 = integerFeatures(5).asInstanceOf[LessThan]

    assertTrue(f1 isSubgroupOf f2)
  }

  @Test
  def less_than_subgroup_false(): Unit = {
    val f1: LessThan = integerFeatures(4).asInstanceOf[LessThan]
    val f2 = integerFeatures(5).asInstanceOf[LessThan]

    assertFalse(f2 isSubgroupOf f1)
    assertFalse(f1 isSubgroupOf f1) // Is not self subgroup
  }

  @Test
  def greater_equal_than_all(): Unit = {
    val f1 = integerFeatures(6).asInstanceOf[GreaterEqualThan]
    val f2 = integerFeatures(7).asInstanceOf[GreaterEqualThan]
    val f3 = new IntegerFeature("C", 1, 1) with GreaterEqualThan
    val f4 = new IntegerFeature("C", 1, 1) with EqualThan

    assertTrue(f1 isSupergroupOf f2)
    assertTrue(f2 isSupergroupOf f2)
    assertTrue(f2 isSubgroupOf f1)
    assertFalse(f1 isSubgroupOf f1)
    assertFalse(f1 isSubgroupOf f2)
    assertFalse(f2 isSupergroupOf f1)

    assertTrue(f1 == f1)
    assertTrue(f1 == f3)
    assertTrue(f1 contain f4)

  }

}
