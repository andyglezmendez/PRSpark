package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality


class ConfidenceQuality extends BaseQuality("ConfidenceQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    return BaseQuality.ValidateResult(ct.N_P_C / ct.N_P)
  }

}
