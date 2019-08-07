package common.quality.qualities

import common.pattern.ContrastPattern
import common.quality.BaseQuality

class C1Quality() extends BaseQuality("C1Quality") {

  val colQuality: ColemanQuality = new ColemanQuality ;
  val cohenQuality: CohenQuality = new CohenQuality ;

  override def getQuality(contrastPattern: ContrastPattern): Double = {
    val ct = contrastPattern.contingencyTable();

    val colQD = colQuality.getQuality(contrastPattern);
    val cohenQD = cohenQuality.getQuality(contrastPattern);

    val result = colQD * (2 + cohenQD) / 3;
    return BaseQuality.ValidateResult(result);
  }
}
