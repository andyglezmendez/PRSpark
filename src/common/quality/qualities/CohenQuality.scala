package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality


class CohenQuality extends BaseQuality("CohenQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val result = (ct.f_P_C + ct.f_nP_nC - (ct.f_P * ct.f_C + ct.f_nP * ct.f_nC)) /
      (1 - (ct.f_P * ct.f_C + ct.f_nP * ct.f_nC))
    return BaseQuality.ValidateResult(result)
  }
}
