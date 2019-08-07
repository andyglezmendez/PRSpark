package common.feature

import common._
import org.junit.{Before, Test}
import org.junit.Assert._

class CategoricalFeatureTest {
  protected var categoricalFeatures: Array[CategoricalFeature] = _

  @Before
  def init(): Unit = {
    categoricalFeatures = Array(
      new CategoricalFeature("Col_1", 1, "Val_1"),
      new CategoricalFeature("Col_2", 2, "Value"),
      new CategoricalFeature("Col_3", 3, "42"),
      new CategoricalFeature("Col_4", 4, "4.2")
    )
  }

  @Test
  def equal_operator_true(): Unit = {

    val f = new CategoricalFeature("Col_1", 1, "Val_1")

    assertTrue(f == categoricalFeatures(0))
  }

  //  @Test
  //  def equal_operator_false(): Unit ={
  //    val f = new CategoricalFeature("Col_1", 1, "Val_1") with DoubleType
  //    val fS = new CategoricalFeature("Col_2", 2, "Value") with IntegerType
  //    val fI = new CategoricalFeature("Col_3", 3, "42") with StringType
  //    val fD = new CategoricalFeature("Col_4", 4, "4.2")
  //
  //    val boolF: Boolean = (f == catFeature) || (fS == catFeatureString) || (fI == catFeatureInteger) || (fD == catFeatureDouble)
  //
  //    assertFalse(boolF)
  //  }

}
