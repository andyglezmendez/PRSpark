package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality

class RuleInterestQuality extends BaseQuality("RuleInterestQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val value = ct.N_P * ct.N_nC / ct.N - ct.N_P_nC
    return BaseQuality.ValidateResult(value)
  }
}
