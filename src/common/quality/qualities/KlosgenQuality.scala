package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality
import common.quality.BaseQuality.squareRoot


class KlosgenQuality extends BaseQuality("KlosgenQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val result = squareRoot(ct.f_P_C) * (ct.f_P_C / ct.f_P - ct.f_C)
    return BaseQuality.ValidateResult(result)
  }

}
