package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality


class ColemanQuality() extends BaseQuality("ColemanQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val result = (ct.f_P_C / ct.f_P - ct.f_C) / (1 - ct.f_C)
    return BaseQuality.ValidateResult(result);
  }

}
