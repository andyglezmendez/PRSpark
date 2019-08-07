package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality

class MultiplyQuality(bq1: BaseQuality, bq2: BaseQuality) extends BaseQuality("MultiplyQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    return BaseQuality.ValidateResult(bq1.getQuality(contrastPattern) * bq2.getQuality(contrastPattern))
  }
}
