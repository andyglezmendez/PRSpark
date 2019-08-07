package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality

class RelativeRiskQuality extends BaseQuality("RelativeRiskQuality") {


  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val result = ct.f_P_C * ct.f_nP / (ct.f_P * ct.f_nP_C)
    return BaseQuality.ValidateResult(result)
  }
}
