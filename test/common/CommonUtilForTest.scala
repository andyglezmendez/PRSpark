package common

//
import common.feature._
import common.pattern.ContrastPattern
import org.junit.Before

class CommonUtilForTest {

  protected var patterns: Array[ContrastPattern] = _
  protected var features: Array[CategoricalFeature] = _
  protected var integerFeatures: Array[IntegerFeature] = _
  protected var categoricalFeature: Array[CategoricalFeature] = _


  @Before
  def initFeatures(): Unit = {
    initCategoricalFeature()
    initIntegerFeature()
    initPatterns()
  }


  def initIntegerFeature(): Unit = {
    integerFeatures = Array(
      new IntegerFeature("A", 0, 1) with EqualThan,
      new IntegerFeature("A", 0, 2) with GreaterThan,
      new IntegerFeature("A", 0, 3) with GreaterThan,
      new IntegerFeature("B", 1, 1) with EqualThan,
      new IntegerFeature("B", 1, 2) with LessThan,
      new IntegerFeature("B", 1, 3) with LessThan,
      new IntegerFeature("C", 1, 1) with GreaterEqualThan,
      new IntegerFeature("C", 1, 2) with GreaterEqualThan,
      new IntegerFeature("D", 1, 1) with LessEqualThan,
      new IntegerFeature("D", 1, 2) with LessEqualThan
    )
  }

  def initCategoricalFeature(): Unit = {
    categoricalFeature = Array(
      new CategoricalFeature("Col_1", 0, "A"),
      new CategoricalFeature("Col_1", 0, "B"),
      new CategoricalFeature("Col_2", 1, "X"),
      new CategoricalFeature("Col_2", 1, "Y")
    )
  }

//  def initPatterns(): Unit = {
//    initPatternFeatures()
//    patterns = Array(
//      new ContrastPattern(Array(features(0), features(3), features(6), features(9)), features(11)),
//      new ContrastPattern(Array(features(1), features(3), features(6), features(10)), features(11)),
//      new ContrastPattern(Array(features(2), features(4), features(6), features(9)), features(11)),
//      new ContrastPattern(Array(features(1), features(3), features(7), features(9)), features(11)),
//
//      new ContrastPattern(Array(features(2), features(4), features(7), features(9)), features(12)),
//      new ContrastPattern(Array(features(2), features(3), features(7), features(9)), features(12)),
//      new ContrastPattern(Array(features(1), features(4), features(8), features(10)), features(12))
//    )
//  }

  def initPatterns(): Unit = {
    initPatternFeatures()
    patterns = Array(
      new ContrastPattern(Array(features(0), features(3), features(6), features(9)), features(11)),
      new ContrastPattern(Array(features(1), features(3), features(6), features(10)), features(11)),
      new ContrastPattern(Array(features(2), features(4), features(6), features(9)), features(11)),
      new ContrastPattern(Array(features(1), features(3), features(7), features(9)), features(11)),

      new ContrastPattern(Array(features(2), features(4), features(7), features(9)), features(12)),
      new ContrastPattern(Array(features(2), features(3), features(7), features(9)), features(12)),
      new ContrastPattern(Array(features(1), features(4), features(8), features(10)), features(12))
    )
  }


  private def initPatternFeatures(): Unit = {
    features = Array(
      new CategoricalFeature("Col_1", 0, "A"), new CategoricalFeature("Col_1", 0, "B"),
      new CategoricalFeature("Col_1", 0, "C"), new CategoricalFeature("Col_2", 1, "1"),
      new CategoricalFeature("Col_2", 1, "2"), new CategoricalFeature("Col_2", 1, "3"),
      new CategoricalFeature("Col_3", 2, "X"), new CategoricalFeature("Col_3", 2, "Y"),
      new CategoricalFeature("Col_3", 2, "Z"), new CategoricalFeature("Col_4", 3, "V"),
      new CategoricalFeature("Col_4", 3, "W"), new CategoricalFeature("Class", 4, "C_1"),
      new CategoricalFeature("Class", 4, "C_2")
    )
  }
}
