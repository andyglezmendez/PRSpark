package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality

class WeightedSumConfidenceAndCoverageQuality extends BaseQuality("WeightedSumConfidenceAndCoverageQuality") {

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val _conf: ConfidenceQuality = new ConfidenceQuality
    val _cover: CoverageQuality = new CoverageQuality


    val cons = _conf.getQuality(contrastPattern)
    val w1 = 0.5 + 0.25 * cons
    val w2 = 0.5 - 0.25 * cons
    return BaseQuality.ValidateResult(w1 * cons + w2 * _cover.getQuality(contrastPattern))
  }

}
