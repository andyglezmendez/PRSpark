package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality

class ExampleCounterExampleRateQuality extends BaseQuality("ExampleCounterExampleRateQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val result = 1 - ct.f_P_nC / ct.f_P_C
    return BaseQuality.ValidateResult(result)
  }
}
