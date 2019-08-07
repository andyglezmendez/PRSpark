package arnold.keel_adapter

class KeelToken {

  var featureName: String = null
  var featureType: String = null
  var featureValues: Array[String] = null
  var isInput = false
  var isOutput = false

  override def toString: String = {
    return featureName + " ::" + featureType
  }
}

object KeelToken {

  def parseLine(tokenStr: String): KeelToken = {
    val result = new KeelToken;
    {
      var indexLeft = tokenStr.indexOf("[");
      var indexRigth = tokenStr.indexOf("]");

      val noInterval = indexLeft == -1 && indexRigth == -1
      var existRealInterval = tokenStr.contains("integer") || tokenStr.contains("real")

      if (existRealInterval)
      {
        val otherValues = tokenStr.split(" ")
        result.featureName = otherValues(0).trim
        result.featureType = otherValues(1).trim

        if (noInterval)
          result.featureValues = new Array[String](0)
        else {
          val rankValueStr = tokenStr.substring(indexLeft + 1, indexRigth)
          val rankValues = rankValueStr.split(",")
          result.featureValues = rankValues;
        }
      }
      else
      {
        indexLeft = tokenStr.indexOf("{")
        indexRigth = tokenStr.indexOf("}")

        if (indexLeft != -1 && indexRigth != -1) {
          val valuesStr = tokenStr.substring(indexLeft + 1, indexRigth)
          result.featureValues = valuesStr.split(",")

          val temp = tokenStr.split(" ")
          result.featureName = temp(0)
        }
      }

      if (result.featureValues != null)
        for (i <- 0 until result.featureValues.length) {
          result.featureValues(i) = result.featureValues(i).trim
        }
    }
    return result
  }

}

