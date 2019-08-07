package common.quality.qualities

import java.util.regex.Pattern

import common.pattern.ContrastPattern
import common.quality.BaseQuality

class AccuracyQuality() extends BaseQuality("AccuracyQuality") {

  override def getQuality(pattern: ContrastPattern): Double = {
    val ct = pattern.contingencyTable()
    val result = ct.f_P_C + ct.f_nP_nC;
    return BaseQuality.ValidateResult(result);
  }

}
