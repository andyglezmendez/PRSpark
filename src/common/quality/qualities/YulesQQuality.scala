package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality

class YulesQQuality extends BaseQuality ( "YulesQQuality"){

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val numerator = ct.f_P_C * ct.f_nP_nC - ct.f_P_nC * ct.f_nP_C
    val denominator = ct.f_P_C * ct.f_nP_nC + ct.f_P_nC * ct.f_nP_C
    val result = numerator / denominator
    return BaseQuality.ValidateResult(result)
  }

}
