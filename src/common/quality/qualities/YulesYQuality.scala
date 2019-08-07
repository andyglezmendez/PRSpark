package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality
import common.quality.BaseQuality.squareRoot


class YulesYQuality extends BaseQuality("YulesYQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable()
    val numerator = squareRoot(ct.f_P_C * ct.f_nP_nC) - squareRoot(ct.f_P_nC * ct.f_nP_C)
    val denominator = squareRoot(ct.f_P_C * ct.f_nP_nC) + squareRoot(ct.f_P_nC * ct.f_nP_C)
    val result = numerator / denominator
    return BaseQuality.ValidateResult(result)
  }

}
