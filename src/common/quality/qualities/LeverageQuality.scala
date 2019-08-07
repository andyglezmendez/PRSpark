package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality

class LeverageQuality extends BaseQuality("LeverageQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val result = ct.f_P_C / ct.f_P - ct.f_P * ct.f_C
    return BaseQuality.ValidateResult(result)
  }

}
