package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality
import common.quality.BaseQuality.squareRoot


class PearsonCorrelationQuality extends BaseQuality("PearsonCorrelationQuality") {


  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val n = ct.f_P_C - ct.f_P * ct.f_C
    val d = squareRoot(ct.f_P * ct.f_nP * ct.f_C * ct.f_nC)
    val result = n / d
    return BaseQuality.ValidateResult(result)
  }

}
