package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality


class GainQuality extends BaseQuality("GainQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    if (ct.f_P_C == 0)
      return 0
    else
      return BaseQuality.ValidateResult(ct.f_P_C * (BaseQuality.log2(ct.f_P_C / ct.f_P) - BaseQuality.log2((ct.f_C))))
  }
}
