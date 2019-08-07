package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality


class ConfidenceYatesCorrectionQuality extends BaseQuality("ConfidenceYatesCorrectionQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    return BaseQuality.ValidateResult((ct.N_P_C - 1d / 2) /ct. N_P)
  }
}
