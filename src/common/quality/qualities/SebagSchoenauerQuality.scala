package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality


class SebagSchoenauerQuality extends BaseQuality("SebagSchoenauerQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val result = ct.f_P_C / ct.f_P_nC
    return BaseQuality.ValidateResult(result)
  }

}
