package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality

class BrinsQuality() extends BaseQuality("BrinsQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val result = (ct.f_P * ct.f_nC) / ct.f_P_nC;
    return BaseQuality.ValidateResult(result);
  }

}
