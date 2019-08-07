package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality

class GrowthRateQuality extends BaseQuality("GrowthRateQuality") {

  override def getQuality(pattern: ContrastPattern): Double = {
    val ct = pattern.contingencyTable()
    return BaseQuality.ValidateResult(ct.DPSupport / ct.DNSupport)
  }
}


