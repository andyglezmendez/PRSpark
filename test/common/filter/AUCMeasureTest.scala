package common.filter

import common.filter.pattern_teams_measures.AUCMeasure
import org.junit.{Assert, Test}

class AUCMeasureTest {
  @Test
  def auc_measure_test(): Unit = {
    val points = Seq((0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0))
    val auc = new AUCMeasure().auc(points)
    Assert.assertEquals(1.0,auc,0.01)
  }
}
