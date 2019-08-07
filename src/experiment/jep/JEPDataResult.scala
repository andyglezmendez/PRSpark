package experiment.jep

import experiment.DataResult

class JEPDataResult(val db: String, val noise: Double, val effectivness: Double, length: Array[(Int,Int)]) extends DataResult{

  val len1 = length.find(_._1 == 1).getOrElse(Tuple2(1,0))

  override def header(): String = "db\tnoise\teffectivness\tlength-1\tlength-2\tlength-3\tlength-4\tlength-5\tlength-6\tlength-7\tlength-8\tlength-9\tlength-10\tpatterns\n"

  override def toString: String ={
    var text = s"${db}\t${noise}\t${effectivness}"

    var patterns = 0
    for(i ← 1 to 10){
      val count = length.find(_._1 == i).getOrElse(Tuple2(i,0))._2
      text = text + s"\t${count}"
      patterns += count
    }
    text = text + s"\t${patterns}"


    return text + "\n"
  }
}
