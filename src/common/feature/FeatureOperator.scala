package common.feature

import org.apache.spark.sql.Row

trait EqualThan extends NumericFeature {

  abstract override def ==(feature: Feature[_]): Boolean = super.==(feature) && feature.isInstanceOf[EqualThan]

  abstract override def isMatch(instance: Row): Boolean = {
    if (super.isMatch(instance)){
//      var result = instance.get(index) match {
//        case num: Double ⇒ num == value
//        case num: Int ⇒ num == value.toInt
//        case _ ⇒ throw new IllegalArgumentException("Type not supported")
//      }
      var result = instance.get(index) == value
      if (reverse) result = !result
      return result
    }
    false
  }
}

trait GreaterThan extends NumericFeature {
  override protected val operator: String = "> "

  abstract override def ==(feature: Feature[_]): Boolean = super.==(feature) && feature.isInstanceOf[GreaterThan]

  abstract override def isMatch(instance: Row): Boolean = {
    if (super.isMatch(instance)) {
      var result = instance.getDouble(index) > value
      if (reverse) result = !result
      return result
    }
    false
  }

  def isSupergroupOf(feature: NumericFeature): Boolean = {
    if (feature.isInstanceOf[GreaterThan]) {
      var result = value <= feature.value
      if (reverse) result = !result
      return result
    }
    false
  }

  def isSubgroupOf(feature: NumericFeature): Boolean = {
    if (feature.isInstanceOf[GreaterThan]) {
      var result = value > feature.value
      if (reverse) result = !result
      return result
    }
    false
  }

  def contain(feature: EqualThan): Boolean = {
    if (feature.attribute == attribute && feature.index == index) {
      var result = feature.value > value
      if (reverse) result = !result
      return result
    }
    false
  }

  def notContain(feature: EqualThan): Boolean = !contain(feature)
}

trait GreaterEqualThan extends NumericFeature {
  override protected val operator: String = ">= "

  abstract override def ==(feature: Feature[_]): Boolean = super.==(feature) && feature.isInstanceOf[GreaterEqualThan]

  abstract override def isMatch(instance: Row): Boolean = {
    if (super.isMatch(instance)){
      var result = instance.getDouble(index) >= value
      if (reverse) result = !result
      return result
    }
    false
  }

  def isSupergroupOf(feature: NumericFeature): Boolean = {
    if (feature.isInstanceOf[GreaterThan] || feature.isInstanceOf[GreaterEqualThan]) {
      var result = value <= feature.value
      if (reverse) result = !result
      return result
    }
    false
  }

  def isSubgroupOf(feature: NumericFeature): Boolean = {
    if (feature.isInstanceOf[GreaterThan] || feature.isInstanceOf[GreaterEqualThan]) {
      var result = value > feature.value
      if (reverse) result = !result
      return result
    }
    false
  }

  def contain(feature: EqualThan): Boolean = {
    if (feature.attribute == attribute && feature.index == index) {
      var result = feature.value >= value
      if (reverse) result = !result
      return result
    }
    false
  }

  def notContain(feature: EqualThan): Boolean = !contain(feature)
}

trait LessThan extends NumericFeature {
  override protected val operator: String = "< "

  abstract override def ==(feature: Feature[_]): Boolean = super.==(feature) && feature.isInstanceOf[LessThan]

  abstract override def isMatch(instance: Row): Boolean = {
    if (super.isMatch(instance)){
      var result = instance.getDouble(index) < value
      if (reverse) result = !result
      return result
    }
    false
  }

  def isSupergroupOf(feature: NumericFeature): Boolean = {
    if (feature.isInstanceOf[LessThan]) {
      var result = value >= feature.value
      if (reverse) result = !result
      return result
    }
    false
  }

  def isSubgroupOf(feature: NumericFeature): Boolean = {
    if (feature.isInstanceOf[LessThan]) {
      var result = value < feature.value
      if (reverse) result = !result
      return result
    }
    false
  }

  def contain(feature: EqualThan): Boolean = {
    if (feature.attribute == attribute && feature.index == index) {
      var result = feature.value < value
      if (reverse) result = !result
      return result
    }
    false
  }

  def notContain(feature: EqualThan): Boolean = !contain(feature)
}

trait LessEqualThan extends NumericFeature {
  override protected val operator: String = "<= "

  abstract override def ==(feature: Feature[_]): Boolean = super.==(feature) && feature.isInstanceOf[LessEqualThan]

  abstract override def isMatch(instance: Row): Boolean = {
    if (super.isMatch(instance)){
      var result = instance.getDouble(index) <= value
      if (reverse) result = !result
      return result
    }
    false
  }

  def isSupergroupOf(feature: NumericFeature): Boolean = {
    if (feature.isInstanceOf[LessThan]) {
      var result = value >= feature.value
      if (reverse) result = !result
      return result
    }
    false
  }

  def isSubgroupOf(feature: NumericFeature): Boolean = {
    if (feature.isInstanceOf[LessThan]) {
      var result = value < feature.value
      if (reverse) result = !result
      return result
    }
    false
  }

  def contain(feature: EqualThan): Boolean = {
    if (feature.attribute == attribute && feature.index == index) {
      var result = feature.value <= value
      if (reverse) result = !result
      return result
    }
    false
  }

  def notContain(feature: EqualThan): Boolean = !contain(feature)

}