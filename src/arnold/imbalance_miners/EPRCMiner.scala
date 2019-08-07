package arnold.imbalance_miners

import common.feature.{CategoricalFeature, Feature}
import common.miner.{DatasetSchema, SparkMiner}
import common.pattern.ContrastPattern
import common.quality.qualities.GrowthRateQuality
import org.apache.spark.sql.Row
import scala.collection.mutable.ArrayBuffer

class EPRCMiner(
                 sparkMiner: SparkMiner,
                 lowerClass: Feature[String],
                 greaterClass: Feature[String],
                 minGrowRait: Integer
               ) extends SparkMiner {

  private var minerBase: SparkMiner = sparkMiner;

  private val minorityClass: Feature[String] = lowerClass;

  private val mayorityClass: Feature[String] = greaterClass;

  private var cutGrowRaithPoint: Integer = minGrowRait;

  override var dataMiner: DatasetSchema = _

  var rankinFeatureValue: ArrayBuffer[BoxClass] = new ArrayBuffer[BoxClass]();

  var initialCountCP = 0;

  override protected def validateArguments(): Unit = {

  }

  def changeFeature(value: Feature[_]): Feature[_] = {

    //Continuar mañana problema a resolver como efectuar el cambio de feature
    var cast = value.asInstanceOf[Feature[String]]
    var newValue: Feature[_] = null;

    var i = 0;
    var found = false;
    while (i < rankinFeatureValue.size && !found) {
      if (rankinFeatureValue(i).feature.attribute == value.attribute) {
        found = true;
        newValue = rankinFeatureValue(i).feature.asInstanceOf[Feature[String]]
      }
      i += 1;
    }

    val result: Feature[String] = new CategoricalFeature(
      cast.attribute,
      cast.index,
      newValue.value.asInstanceOf[String],
      cast.reverse
    )
    return result;
  }

  def buildRankingValues(dataMiner: DatasetSchema, rows: Array[Row]): Unit = {

    var countOfInstancesOfMinorityClass = 0;
    var countOfInstancesOfMayorityClass = 0;

    for (r <- rows)
      if (minorityClass.isMatch(r))
        countOfInstancesOfMinorityClass += 1;
      else if (mayorityClass.isMatch(r))
        countOfInstancesOfMayorityClass += 1;

    val features = dataMiner.features();
    for (f <- features) {
      rankinFeatureValue += new BoxClass(f, dataMiner, minorityClass, mayorityClass, countOfInstancesOfMinorityClass, countOfInstancesOfMayorityClass);
    }
    for (r <- rows)
      for (rf <- rankinFeatureValue) {
        rf.featureMatchWith(r);
        rf.growRateValue();
      }

    for (f <- rankinFeatureValue)
      f.growRateValue();

    rankinFeatureValue = rankinFeatureValue.sortWith(_.growRate > _.growRate);

    var finalRanking = new ArrayBuffer[BoxClass]();
    for (f <- rankinFeatureValue) {
      val contains = containsFeatureAttribute(f.feature, finalRanking);
      if (!contains && f.feature.attribute != "Class")
        finalRanking += f
    }

    rankinFeatureValue = finalRanking;
    rankinFeatureValue = rankinFeatureValue.sortWith(_.growRate > _.growRate);
  }

  private def containsFeatureAttribute(feature: Feature[_], list: ArrayBuffer[BoxClass]): Boolean = {
    var result: Boolean = false;
    var i = 0;
    while (i < list.size && !result) {
      val f = list(i);
      if (f.feature.attribute == feature.attribute)
        result = true;
      i += 1;
    }
    return result;
  }

  override def mine(rows: Array[Row]): Array[ContrastPattern] = {
    dataMiner = new DatasetSchema(rows);

    //Construye el ranking de los valores de los features basado en el
    // grow rate de los mismos.
    buildRankingValues(dataMiner, rows);

    var result: Array[ContrastPattern] = minerBase.mine(rows);
    // Guardo la cantidad inicial de patrones
    initialCountCP = result.length;
    {
      var lowerPatterns: ArrayBuffer[ContrastPattern] = new ArrayBuffer[ContrastPattern]();
      //Guardo en lowerPatterns todos los patrones que pertenecen a la clase minoritaria
      for (pattern: ContrastPattern <- result) {
        if (pattern.clazz == minorityClass) {
          lowerPatterns += pattern;
        }
      }

      //Por cada patron de la clase minoritaria
      for (pat <- lowerPatterns) {
        val predicate: Array[Feature[_]] = pat.predicate;

        //Por cada item en el ranking
        var i = 0;
        while (i < rankinFeatureValue.size && i < pat.predicate.length) {

          // Temporal contiene el predicado del patron que no ha cambiado
          // y en i esta guardado el Feature al cual hay que cambiarle
          // su valor
          val temp: ArrayBuffer[Feature[_]] = new ArrayBuffer[Feature[_]];

          //Copio los valores del predicado a temporal.
          for (item <- pat.predicate)
            temp += item;

          //Cambio el valor del Feature correspondiente
          val featureToChange = changeFeature(temp(i));
          temp.update(i, featureToChange);

          //Agrego el nuevo patron a la coleccion de patrones que responden a la clase minoritaria
          val newPattern: ContrastPattern = new ContrastPattern(temp.toArray, minorityClass);

          //Añado el nuevo patron si no esta entre los patrones de la clase minoritaria
          //de esta forma si esta duplicado no se añade por gusto
          if (!lowerPatterns.contains(newPattern))
            lowerPatterns += newPattern;

          i += 1;
        }
      }

      //Elimino los patrones cuyo Grow_Rate es mas bajo que el punto de corte
      var finalSet: ArrayBuffer[ContrastPattern] = new ArrayBuffer[ContrastPattern]();

      //Le agrego a finalSet los patrones que estan en result
      for (pattern <- result)
        finalSet += pattern

      //Le agrego a finalSet aquellos patrones que pertenecen a la clase minoritaria que no estan en result.
      for (p <- lowerPatterns)
        if (!finalSet.contains(p))
          finalSet += p;

      var i = 0;
      while (i < finalSet.size) {

        val growthRateQuality = new GrowthRateQuality
        //Calculo el grow rate del patron
        val valueGrowRate = growthRateQuality.getQuality(finalSet(i))
        //Remuevo los patrones cuyo grow rate esta por debajo del punto de corte
        if (valueGrowRate < cutGrowRaithPoint)
          finalSet -= finalSet(i)

        i += 1;
      }

      //Actualizo result para que contenga los patrones resultantes
      result = finalSet.toArray
    }
    return result;
  }
}
