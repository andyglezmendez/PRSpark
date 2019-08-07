package experiment.minimal

import experiment.DataResult

class MinimalDataResult(val db: String, val effectivness: Double, minPatterns: Int, support: Double) extends DataResult {

  override def header(): String = "db\teffectivness\tpatterns\tsupp\n"

  override def toString: String = s"${db}\t${effectivness}\t${minPatterns}\t${support}\n"
}
