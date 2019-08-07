package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality

class WraccQuality extends BaseQuality("WraccQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val value = ct.N_P / ct.N * (ct.N_P_C / ct.N_P - ct.N_C / ct.N)
    return BaseQuality.ValidateResult(value)
  }
}
