package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality


class CausalConfidenceQuality extends BaseQuality("CausalConfidenceQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val result = 1 - 0.5 * (ct.f_P_nC / ct.f_P + ct.f_P_nC / ct.f_nC)
    return BaseQuality.ValidateResult(result)
  }

}
