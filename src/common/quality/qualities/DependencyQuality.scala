package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality

class DependencyQuality extends BaseQuality("DependencyQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val value = Math.abs(ct.N_nC / ct.N - ct.f_P_nC / ct.N_P)
    return BaseQuality.ValidateResult(value)
  }

}
