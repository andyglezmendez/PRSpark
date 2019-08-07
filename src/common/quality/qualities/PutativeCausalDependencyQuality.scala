package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality


class PutativeCausalDependencyQuality extends BaseQuality("PutativeCausalDependencyQuality"){

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val value = 3d / 2d + (4d * ct.N_P - 3d * ct.N_C) / (2d * ct.N) - (3d / 2d / ct.N_P + 2d / ct.N_nC) * ct.N_P_nC
    return BaseQuality.ValidateResult(value)
  }

}
