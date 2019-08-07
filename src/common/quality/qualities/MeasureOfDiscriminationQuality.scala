package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality
import common.quality.BaseQuality.log2


class MeasureOfDiscriminationQuality extends BaseQuality("MeasureOfDiscriminationQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable()
    val result = log2((ct.N_P_C / ct.N_nP_C) / (ct.N_P_nC / ct.N_nP_nC))
    return BaseQuality.ValidateResult(result)
  }


}
