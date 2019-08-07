package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality


class CollectiveStrengthQuality extends BaseQuality("ColectiveStrengthQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();

    val n1 = ct.f_P_C + ct.f_nP_nC
    val d1 = ct.f_P * ct.f_C + ct.f_nP * ct.f_nC
    val n2 = 1 - ct.f_P * ct.f_C - ct.f_nP * ct.f_nC
    val d2 = 1 - ct.f_P_C - ct.f_nP_nC

    val result = n1 / d1 * n2 / d2
    return BaseQuality.ValidateResult(result)
  }

}
