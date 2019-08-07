package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality

class CosineQuality extends BaseQuality("CosineQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val result = ct.f_P_C / Math.sqrt(ct.f_P * ct.f_C)
    return BaseQuality.ValidateResult(result)
  }

}
