package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality


class CausalConfirmedConfidenceQuality extends BaseQuality ("CausalConfirmedConfidenceQuality"){

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val value = 1 - 0.5 * (3 / ct.N_P + 1 / ct.N_nC) * ct.N_P_nC
    return BaseQuality.ValidateResult(value)
  }

}
