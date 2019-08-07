package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality

class InformationGainQuality extends BaseQuality("InformationGainQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val result = -Math.log(ct.N_C / ct.N) + Math.log(ct.N_P_C / ct.N_P)
    return BaseQuality.ValidateResult(result)
  }

}
