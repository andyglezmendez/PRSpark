package common.border

import common.CommonUtilForTest
import org.junit.{Before, Test}
import org.junit.Assert._

class BorderOperatorTest extends CommonUtilForTest {

  @Before
  def init(): Unit = {
    initPatterns()
  }

  @Test
  def border_diff(): Unit = {
    val pattern = patterns(0)
    val complement = patterns.slice(1, 4)
    val left = BorderOperator.borderDiff(pattern, complement).get

    left.foreach(println(_))

    val p1 = left(0).predicate.sameElements(pattern.predicate.slice(0, 1))
    val p2 = left(1).predicate.sameElements(pattern.predicate.slice(1, 4))
    assertTrue(p1 && p2)
  }

  @Test
  def all_borders(): Unit = {
    val borders = BorderOperator.allBorders(patterns, Array(features(11), features(12)))

    var i = 1
    for (group ← borders){
      println(s"Group $i")
      println("LEFT")
      group._1.foreach(println(_))
      println("RIGHT")
      group._2.foreach(println(_))
      println()
      i += 1
    }

    patterns.foreach(println)

    assertEquals(6, borders(0)._1.length)
    assertEquals(4, borders(0)._2.length)

    assertEquals(6, borders(1)._1.length)
    assertEquals(3, borders(1)._2.length)
  }

}
