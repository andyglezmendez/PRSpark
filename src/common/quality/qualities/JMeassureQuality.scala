package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality

class JMeassureQuality extends BaseQuality("JMeassureQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();

    if ((ct.f_P_C == 0) || (ct.f_P_nC == 0)) return 1
    val s1 = ct.f_P_C * Math.log(ct.f_P_C / ct.f_P / ct.f_C)
    val s2 = ct.f_P_nC * Math.log(ct.f_P_nC / ct.f_P / ct.f_nC)
    val result = s1 + s2
    return BaseQuality.ValidateResult(result)
  }

}
