package common.utils

class BitArray(private val size: Int) {

  val values = new Array[Boolean](size)

  def apply(index: Int): Boolean = values(index)

  def update(index: Int, value: Boolean): Unit = {
    values(index) = value
  }

  def length() = size

  def and(array: BitArray): BitArray = {
    val arr = new BitArray(size)
    for (i ← values.indices) {
      arr(i) = values(i) && array(i)
    }
    arr
  }

  def or(array: BitArray): BitArray = {
    val arr = new BitArray(size)
    for (i ← values.indices) {
      arr(i) = values(i) || array(i)
    }
    arr
  }

  def setAllValues(value: Boolean): Unit ={
    for(i ← values.indices)
      values(i) = value
  }

  def count(count: (Boolean ⇒ Boolean)) = values.count(count)

  def toArray(): Array[Boolean] = values.clone()

  override def equals(obj: scala.Any): Boolean = {
    if(!obj.isInstanceOf[BitArray]) return false
    val arr = obj.asInstanceOf[BitArray]
    if(arr.length != this.length) return false

    for(i ← this.values.indices)
      if(values(i) != arr(i)) return false

    true
  }

  override def toString: String = values.mkString("(", ",", ")")

}
