package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality

class C2Quality extends BaseQuality("C2Quality") {

  val colemanQuality: ColemanQuality = new ColemanQuality
  val coverageQuality: CoverageQuality = new CoverageQuality

  override def getQuality(contrastPattern: ContrastPattern): Double = {

    val colQuality = colemanQuality.getQuality(contrastPattern);
    val covQuality = coverageQuality.getQuality(contrastPattern);

    val result = colQuality * (1 + covQuality / 2)
    return BaseQuality.ValidateResult(result)
  }
}
