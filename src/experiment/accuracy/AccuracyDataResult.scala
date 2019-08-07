package experiment.accuracy

import experiment.DataResult

class AccuracyDataResult(val db: String, val support: Double, val accuracy: Double, val patternsCount: Int) extends DataResult {

  override def header(): String = "db\tsupp\teffectivness\tpatterns\n"

  override def toString: String = s"${db}\t${support}\t${accuracy}\t${patternsCount}\n"
}
