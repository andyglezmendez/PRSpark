package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality
import common.quality.BaseQuality.squareRoot

class LermanQuality extends BaseQuality("LermanQuality"){

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val n = ct.N_P - ct.N_P_nC - (ct.N_P * ct.N_C) / ct.N
    val d = squareRoot((ct.N_P * ct.N_C) / ct.N)
    val value = n / d
    return BaseQuality.ValidateResult(value)
  }

}
