package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality


class OddsRatioQuality extends BaseQuality("OddsRatioQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    return BaseQuality.ValidateResult(ct.f_P_C * ct.f_nP_nC / (ct.f_P_nC * ct.f_nP_C));
  }

}
