package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality

class ZhangQuality extends BaseQuality("ZhangQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable()
    val n = ct.f_P_C - ct.f_P * ct.f_C
    val d = Math.max(ct.f_P_C * ct.f_nC, ct.f_C * ct.f_P_nC)
    val result = n / d
    return BaseQuality.ValidateResult(result)
  }
}
