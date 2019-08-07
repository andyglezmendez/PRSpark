package common.filter

import common.pattern.ContrastPattern

import scala.collection.mutable.ArrayBuffer

/**
  * Minimal patterns filter extract all minimal patterns from an
  * array of ContrastPattern.
  */
object MinimalPatternsFilter extends Serializable {

  /**
    * Filter minimal patterns.
    *
    * @param patterns Array of ContrastPatterns
    * @return Array of minimal patterns
    */
  def filter(patterns: Array[ContrastPattern]): Array[ContrastPattern] = {
    val minimals = ArrayBuffer.empty[ContrastPattern]

    for (pattern ← patterns) {
      var isMinimal = true
      var c = 0
      while (minimals.length > c && isMinimal) {
        if (pattern.isSuperPatternOf(minimals(c))) {
          isMinimal = false
          c += 1
        }
        else if (pattern.isSubPatternOf(minimals(c)))
          minimals.remove(c)
        else c += 1
      }
      if (isMinimal)
        minimals += pattern
    }

    minimals.toArray
  }
}
