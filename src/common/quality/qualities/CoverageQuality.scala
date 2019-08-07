package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality

class CoverageQuality extends BaseQuality("CoverageQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val result = ct.N_P_C / ct.N_C;
    return BaseQuality.ValidateResult(result)
  }

}
