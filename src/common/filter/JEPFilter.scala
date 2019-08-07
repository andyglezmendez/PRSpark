package common.filter

import common.pattern.ContrastPattern

/**
  * Jumping Emerging Pattern filter.
  * JEP are patterns with infinite growth rate. That is zero support
  * in the negative class and greater than zero support in the
  * positive class
  */
class JEPFilter extends Serializable {

  /**
    * Filter JEP patterns.
    * @param patterns Array of ContrastPatterns
    * @return JEPs
    */
  def filter(patterns: Array[ContrastPattern]): Array[ContrastPattern] = {
    patterns.filter(pattern ⇒ pattern.growthRate() == Double.PositiveInfinity)
  }
  
}
