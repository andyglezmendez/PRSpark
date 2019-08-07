package common.quality

import common.pattern.ContrastPattern

abstract class BaseQuality(nameQuality: String) extends Serializable {

  /*
  Inner class attributes
   */
  val name: String = nameQuality

  /**
    * This method get's the quality of something
    *
    * @return A double that represents the quality value
    */
  def getQuality(pattern: ContrastPattern): Double

  override def toString: String = name
}

object BaseQuality {
  val max = 6.02E23

  def ValidateResult(quality: Double): Double = {
    var result = -1d;
    {
      if (Double.NegativeInfinity == quality)
        result = -BaseQuality.max
      else if (Double.PositiveInfinity == quality)
        result = BaseQuality.max
      else result = Math.round(Math.min(quality, BaseQuality.max));
    }
    return result;
  }

  def log2(x: Double): Double = {
    return Math.log10(x) / Math.log10(2);
  }

  def pow2(x: Double): Double = {
    return x * x
  }

  def squareRoot(x:Double):Double={
    return Math.sqrt(x)
  }
}