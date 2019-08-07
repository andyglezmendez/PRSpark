package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality
import common.quality.BaseQuality.pow2


class StrengthQuality extends BaseQuality("StrengthQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val n = pow2(ct.f_P_C / ct.f_C)
    val d = ct.f_P_C / ct.f_C + ct.f_P_nC / ct.f_nC
    return BaseQuality.ValidateResult(n / d)
  }

}
