package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality
import common.quality.BaseQuality.squareRoot


class ImplicationIndexQuality extends BaseQuality("ImplicationIndexQuality"){

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();

    val n = ct.N_P * ct.N_C - ct.N * ct.N_P_C
    val d = squareRoot(ct.N * ct.N_P * ct.N_nC)
    val result = -n / d
    return BaseQuality.ValidateResult(result)
  }
}
