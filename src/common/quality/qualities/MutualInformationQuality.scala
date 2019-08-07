package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality
import common.quality.BaseQuality.log2


class MutualInformationQuality extends BaseQuality("MutualInformationQuality")
{

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    var result = 0d
    if (ct.f_P_C > 0) result += ct.f_P_C * log2(ct.f_P_C / (ct.f_P * ct.f_C))
    if (ct.f_P_nC > 0) result += ct.f_P_nC * log2(ct.f_P_nC / (ct.f_P * ct.f_nC))
    if (ct.f_nP_C > 0) result += ct.f_nP_C * log2(ct.f_nP_C / (ct.f_nP * ct.f_C))
    if (ct.f_nP_nC > 0) result += ct.f_nP_nC * log2(ct.f_nP_nC / (ct.f_nP * ct.f_nC))
    return BaseQuality.ValidateResult(result)
  }

}
