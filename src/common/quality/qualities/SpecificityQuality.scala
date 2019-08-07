package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality

class SpecificityQuality extends BaseQuality("SpecificityQuality"){

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val result = ct.f_nP_nC / ct.f_nP
    return BaseQuality.ValidateResult(result)
  }

}
