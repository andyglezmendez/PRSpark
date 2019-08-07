package common.quality

import common.pattern.ContrastPattern

class ContingencyTable(pp: Double, pn: Double, np: Double, nn: Double) extends Serializable {
  /**
    * Contingency Table
    *
    * N -> Total of instances
    *
    * Natural value
    *
    * N_P -> Positive the predicate
    * N_nP -> Negative the predicate
    * N_C -> Positive the class
    * N_nC -> Negative the predicate
    *
    * N_P_C -> Positive the predicate, positive the class
    * N_P_nC -> Positive the predicate, negative the class
    * N_nP_C -> Negative the predicate, positive the class
    * N_nP_nC -> Negative the predicate, negative the class
    *
    * Fraction value (N_* / N)
    *
    * f_P -> Positive the predicate
    * f_nP -> Negative the predicate
    * f_C -> Positive the class
    * f_nC -> Negative the predicate
    *
    * f_P_C -> Positive the predicate, positive the class
    * f_P_nC -> Positive the predicate, negative the class
    * f_nP_C -> Negative the predicate, positive the class
    * f_nP_nC -> Negative the predicate, negative the class
    *
    */

  def this(pattern: ContrastPattern) = this(pattern.pp, pattern.pn, pattern.np, pattern.nn)

  var N_P_C: Double = pp
  var N_P_nC: Double = pn
  var N_nP_C: Double = np
  var N_nP_nC: Double = nn

  var N_P = N_P_C + N_P_nC
  var N_nP = N_nP_C + N_nP_nC
  var N_C = N_nP_C + N_P_C;
  var N_nC = N_nP_nC + N_P_nC

  var N = N_P + N_nP

  var f_P_C = N_P_C / N
  var f_P_nC  = N_P_nC / N
  var f_nP_C = N_nP_C / N
  var f_nP_nC = N_nP_nC / N

  var f_P = f_P_C + f_P_nC
  var f_nP = f_nP_C + f_nP_nC
  var f_C = f_nP_C + f_P_C
  var f_nC = f_nP_nC + f_P_nC

  var DPSupport = N_P_C / N_C
  var DNSupport = N_P_nC / N_nC

  val MaxQuality = 6.02E23

  private def calcContingenceValues(): Unit = {

    N_P = N_P_C + N_P_nC
    N_nP = N_nP_C + N_nP_nC
    N_C = N_nP_C + N_P_C
    N_nC = N_nP_nC + N_P_nC
    N = N_P + N_nP

    f_P_C = N_P_C / N
    f_P_nC = N_P_nC / N
    f_nP_C = N_nP_C / N
    f_nP_nC = N_nP_nC / N
    //    f_P = N_P / N
    //    f_nP = N_nP / N
    //    f_C = N_C / N
    //    f_nC = N_nC / N

    f_P = f_P_C + f_P_nC
    f_nP = f_nP_C + f_nP_nC
    f_C = f_nP_C + f_P_C
    f_nC = f_nP_nC + f_P_nC

    DPSupport = N_P_C / N_C
    DNSupport = N_P_nC / N_nC
  }

}

//trait QualityMeasures extends ContingencyTable {
//  var metricMap = new HashMap[String, Double]
//}

