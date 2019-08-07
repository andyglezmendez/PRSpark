package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality


class PrevalecenceQuality extends BaseQuality("PrevalecenceQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val result = ct.f_C
    return BaseQuality.ValidateResult(result)
  }
}
