package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality

class SupportDifferenceQuality extends BaseQuality("SupportDifferenceQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable()
    return BaseQuality.ValidateResult(ct.f_P_C / ct.f_C - ct.f_P_nC / ct.f_nC)
  }

}
