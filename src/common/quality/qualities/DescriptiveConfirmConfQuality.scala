package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality

class DescriptiveConfirmConfQuality extends BaseQuality ("DescriptiveConfirmConfQuality"){

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val value = 1 - 2 * ct.N_P_nC / ct.N_P
    return BaseQuality.ValidateResult(value)
  }

}
