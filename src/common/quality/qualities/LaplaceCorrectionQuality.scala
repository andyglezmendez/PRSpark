package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality

class LaplaceCorrectionQuality extends BaseQuality( "LaplaceCorrectionQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val result = (ct.N_P_C + 1) / (ct.N_P + 2)
    return BaseQuality.ValidateResult(result)
  }

}
