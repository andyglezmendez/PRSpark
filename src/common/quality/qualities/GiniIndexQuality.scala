package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality
import common.quality.BaseQuality.pow2

class GiniIndexQuality extends BaseQuality ("GiniIndexQuality"){

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable()
    val p1 = ct.f_P * (pow2(ct.f_P_C / ct.f_P) + pow2(ct.f_P_nC / ct.f_P))
    val p2 = ct.f_nP * (pow2(ct.f_nP_C / ct.f_nP) + pow2(ct.f_nP_nC / ct.f_nP))
    val result = p1 + p2 - pow2(ct.f_C) - pow2(ct.f_nC)
    return BaseQuality.ValidateResult(result)
  }

}
