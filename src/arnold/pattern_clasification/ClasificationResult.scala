package classifiers.pattern_clasification

import common.feature.Feature
import org.apache.spark.sql.Row

class ClasificationResult(clasificationProcessResult:Array[Double], feature: Feature[_],inst:Row){

  val clasificationArray : Array[Double] = clasificationProcessResult
  val clazzFeature = feature
  val instance = inst
}
