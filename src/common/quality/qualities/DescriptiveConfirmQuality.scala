package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality

class DescriptiveConfirmQuality extends BaseQuality("DescriptiveConfirmQuality"){

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val value = (ct.N_P - 2 * ct.N_P_nC) / ct.N
    return BaseQuality.ValidateResult(value)
  }

}
