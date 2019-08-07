package common.utils

object MathLog {

  /**
    * Calculate the logarithm of num in base base.
    * @param num The numerator of logarithm
    * @param base The base o logarithm
    * @return The computed logarithm
    */
  def logB(num: Double, base: Double): Double = {
    math.log(num) / math.log(2)
  }
}
