package arnold.imbalance_miners

import common.feature.Feature
import common.miner.DatasetSchema
import common.quality.BaseQuality
import org.apache.spark.sql.Row

class BoxClass(
                featureTarget: Feature[_],
                sch: DatasetSchema,
                minorityClassF: Feature[_],
                mayorityClassF: Feature[_],
                countOfInstancesOfMinorityClass : Integer,
                countOfInstancesOfMayorityClass : Integer
              ) {

  var countMayorityClass: Double = 0;// Cantidad de rows que machea en la clase mayoritaria con el feature del box
  var countMinorityClass: Double = 0;// Cantidad de rows que machea en la clase minoritaria con el feature del box
  var growRate: Double = -1d;

  val schema: DatasetSchema = sch;
  val feature: Feature[_] = featureTarget;
  val minorityClass: Feature[_] = minorityClassF;
  val mayorityClass: Feature[_] = mayorityClassF;
  val countMinClass: Integer = countOfInstancesOfMinorityClass; // Cantidad de instancias que pertenecen a la clase minoritaria
  val countMaxClass: Integer = countOfInstancesOfMayorityClass; // Cantidad de instancias que pertenecen a la clase mayoritaria

  def featureMatchWith(row: Row): Unit = {
    if (feature.isMatch(row)) {
      if (this.minorityClass.isMatch(row))
        countMinorityClass += 1;
      if (this.mayorityClass.isMatch(row))
        countMayorityClass += 1;
    }
  }

  def growRateValue(): Double = {
    growRate = (countMinorityClass/countMinClass) / (countMayorityClass/countMaxClass);
    growRate = BaseQuality.ValidateResult(growRate);
    return growRate;
  }
}
