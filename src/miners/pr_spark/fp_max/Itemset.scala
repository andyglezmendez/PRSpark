package miners.pr_spark.fp_max

import common.feature.{CategoricalFeature, Feature}

import scala.collection.mutable.ArrayBuffer

private[fp_max] class Itemset() extends Serializable {
  var items: ArrayBuffer[Item] = new ArrayBuffer[Item]

  def this(it: Iterable[Item]) {
    this()
    for (item ← it) {
      addItem(item)
    }
  }

  def addItem(item: Item): Boolean = {
    if (items.contains(item)) return false
    items += item
    items = items.sorted
    true
  }

  //TODO Confirmar Un itemset puede ser superset y subset de el mismo?
  def isSubset(other: Itemset): Boolean = {
    if (items.size > other.items.size) return false
    items.forall(other.items.contains(_))
  }

  def isSuperset(other: Itemset): Boolean = {
    if (other.items.size > items.size) return false
    other.items.forall(items.contains(_))
  }

  def ==(other: Itemset): Boolean = {
    if (other.items.size != items.size) return false
    items.sameElements(other.items)
  }

}

private[fp_max] class Item(var value: Double, var support: Double, var feature: CategoricalFeature) extends Ordered[Item] with Serializable {

  override def toString: String = s"${feature.attribute}=${feature.value}"

  def ==(item: Item): Boolean = equals(item)

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case item: Item ⇒
        return item.value == value && item.feature == feature && item.support == support
      case _ ⇒ return false
    }
  }

  override def compare(that: Item): Int = {
    val comp = this.feature.index compare that.feature.index
    if (comp == 0) return this.value compare that.value
    comp
  }

  override def hashCode(): Int = s"$feature = $value".hashCode
}