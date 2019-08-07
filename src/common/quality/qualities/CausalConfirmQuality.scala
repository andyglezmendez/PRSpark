package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality


class CausalConfirmQuality extends BaseQuality("CausalConfirmQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val value = (ct.N_P + ct.N_nC - 4 * ct.N_P_nC) / ct.N
    return BaseQuality.ValidateResult(value)
  }

}
