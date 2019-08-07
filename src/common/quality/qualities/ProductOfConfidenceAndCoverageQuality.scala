package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality

class ProductOfConfidenceAndCoverageQuality extends BaseQuality ("ProductOfConfidenceAndCoverageQuality"){

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();
    val _conf: ConfidenceQuality = new ConfidenceQuality
    val _cover: CoverageQuality = new CoverageQuality

    return BaseQuality.ValidateResult(_conf.getQuality(contrastPattern) * f(_cover.getQuality(contrastPattern)))
  }

  private def f(arg: Double) = Math.exp(arg - 1)
}
