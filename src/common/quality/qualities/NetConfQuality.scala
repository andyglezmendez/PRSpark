package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality

class NetConfQuality extends BaseQuality ("NetConfQuality"){

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val result = (ct.f_P_C - ct.f_P * ct.f_C) / (ct.f_P * (1 - ct.f_P))
    return BaseQuality.ValidateResult(result)
  }

}
