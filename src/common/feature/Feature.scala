package common.feature

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataTypes.{StringType, IntegerType, DoubleType}


/**
  * Feature is an interface that represent a pair column(attribute) value
  */
trait Feature[A] extends Serializable{
  val id: Int
  val attribute: String
  val index: Int
  val reverse: Boolean

  val value: A

  def isMatch(instance: Row): Boolean

  def ==(feature: Feature[_]): Boolean

//  override def equals(obj: scala.Any): Boolean = {
//    if(obj.isInstanceOf[Feature[_]]){
//      return this == obj.asInstanceOf[Feature[_]]
//    }
//    false
//  }

}

//todo Test match function
/**
  * A feature with a categorical value
  *
  * @param attribute String with the column name
  * @param index     Int with the column position
  * @param value     String with the value of the column
  */
class CategoricalFeature(override val attribute: String,
                         override val index: Int,
                         val value: String,
                         override val reverse: Boolean = false) extends Feature[String] {

  val id: Int = s"$attribute $index $value $reverse".hashCode

  /**
    * Take a Row[index] and compare it with the value in the Feature
    *
    * @param instance a Row
    * @return true if the field in Row[index] is the same in the value,
    *         false if do not
    */
  override def isMatch(instance: Row): Boolean = {
    val column = instance.schema.fields(index)
    if (column.dataType == StringType && column.name == attribute) {
      var result = instance.getString(index) == value
      if (reverse) result = !result
      return result
    }
    false
  }

  override def ==(feature: Feature[_]): Boolean = {
    feature match {
      case f: CategoricalFeature ⇒ this.id == f.id
      case _ ⇒ false
    }
  }

  override def toString: String = s"$attribute( ${if (reverse) "!" else ""}$value )"

}

trait NumericFeature extends Feature[Double] {

  override val id: Int = s"$attribute $index $value $reverse $operator".hashCode
  protected val operator: String = ""
  val value: Double

  override def ==(feature: Feature[_]): Boolean = {
    if (feature.isInstanceOf[NumericFeature]) {
      return id == feature.id
    }
    false
  }

  override def toString: String = s"$attribute( ${if (reverse) "!" else ""}$operator$value )"
}

/**
  * A feature with a numeric value like x > value
  *
  * @param attribute String with the column name
  * @param index     Int with the column position
  * @param value     String with the value of the column
  */
class IntegerFeature(override val attribute: String,
                     override val index: Int,
                     override val value: Double,
                     override val reverse: Boolean = false) extends NumericFeature {

  /**
    * Take a Row[index] and compare it with the value in the Feature
    *
    * @param instance a Row
    * @return true if the field in Row[index] is greater than value,
    *         false if not
    */
  override def isMatch(instance: Row): Boolean = {
    instance.schema.fields(index).dataType match {
      case IntegerType ⇒ instance.schema.fields(index).name.equals(attribute)
      case _ ⇒ false
    }
  }

}

/**
  * A feature with a numeric value like x < value
  *
  * @param attribute String with the column name
  * @param index     Int with the column position
  * @param value     String with the value of the column
  */
class DoubleFeature(override val attribute: String,
                    override val index: Int,
                    override val value: Double,
                    override val reverse: Boolean = false) extends NumericFeature {

  /**
    * Take a Row[index] and compare it with the value in the Feature
    *
    * @param instance a Row
    * @return true if the field in Row[index] is less than value,
    *         false if not
    */
  override def isMatch(instance: Row): Boolean = {
    instance.schema.fields(index).dataType match {
      case DoubleType ⇒ instance.schema.fields(index).name.equals(attribute)
      case _ ⇒ false
    }
  }

}

class NumericRangeFeature(override val attribute: String,
                          override val index: Int,
                          val lowerBound: Double,
                          val upperBound: Double,
                          override val reverse: Boolean = false) extends Feature[Array[Double]] {

  val value = Array(lowerBound,upperBound)

  override val id: Int = s"$attribute $index $lowerBound $upperBound $reverse".hashCode

  override def ==(feature: Feature[_]): Boolean = feature.id == id

  override def isMatch(instance: Row): Boolean = {
    val column = instance.schema.fields(index)
    if (column.dataType != StringType && column.name == attribute) {
      var result = lowerBound <= instance.getDouble(index) && instance.getDouble(index) <= upperBound
      if (reverse) result = !result
      return result
    }
    false
  }

  override def toString: String = s"$attribute( ${if (reverse) "!" else ""}[$lowerBound, $upperBound] )"

}