package miners.pr_spark.fp_max

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, ListBuffer}

private[fp_max] class FPNode() extends Serializable {

  var value: Item = _
  var parent: FPNode = _
  var childs: ArrayBuffer[FPNode] = new ArrayBuffer[FPNode]()
  var counter: Int = 0
  var nextHeader: FPNode = _

  def this(value: Item, counter: Int) {
    this()
    this.value = value
    this.counter = counter
  }

  def this(value: Item) {
    this(value, 1)
  }

  def addChild(child: FPNode): Unit = {
    child.parent = this
    childs += child
  }

  def childWithValue(item: Item): FPNode = {
    childs.find(_.value == item).orNull
  }

  override def toString: String = s"($counter)$value"
}

private[fp_max] class FPTree extends Serializable {
  var root: FPNode = new FPNode()
  //Header Table that point to the first FPNode
  var headerTable: mutable.ListMap[Item, FPNode] = new mutable.ListMap[Item, FPNode]
  //Dictionary to the last FPNode pointer
  var lastNodePointer: mutable.HashMap[Item, FPNode] = new mutable.HashMap[Item, FPNode]

  def addTransaction(transaction: Itemset): Unit = {
    val trans = transaction
//    trans.items = trans.items.sortBy(item ⇒ item.support).reverse
    var currentNode = root
    for (item ← trans.items) {
      var child = currentNode.childWithValue(item)

      if (child == null) {
        child = new FPNode(item)
        currentNode.addChild(child)
        fixNodeLinks(child)
      }
      else child.counter += 1

      currentNode = child
    }
  }

  def fixNodeLinks(newNode: FPNode): Unit = {
    val itemNode = lastNodePointer.find(pair ⇒ pair._1 == newNode.value).orNull

    if (itemNode == null) headerTable.+=((newNode.value, newNode))
    else itemNode._2.nextHeader = newNode

    lastNodePointer.+=((newNode.value, newNode))
  }

  def buildHeaderTable(items: Iterable[Item]): Unit = {
    for (item ← items) {
      headerTable.+=((item, null))
    }
  }

  def addMultipleTransactions(transactions: Iterable[Itemset]): Unit = {
    for (itemset ← transactions) {
      addTransaction(itemset)
    }
  }

  def addPrefixPaths(prefixPaths: Iterable[ArrayBuffer[FPNode]], betaSupport: mutable.HashMap[Item, Double], minCount: Int): Unit = {

    val signal = betaSupport.values.exists(_ >= minCount)
    if (!signal) return
    for (prefixPath ← prefixPaths) {
      var currentNode = root
      var pathCounter = prefixPath.head.counter
      prefixPath.remove(0)
      val reversePrefixPath = prefixPath.reverseIterator

      for (node ← reversePrefixPath) {
        if (betaSupport(node.value) >= minCount) {
          var child = currentNode.childWithValue(node.value)

          if (child == null) {
            child = new FPNode(node.value, pathCounter)
            currentNode.addChild(child)
            fixNodeLinks(child)
          }
          else child.counter += pathCounter
          currentNode = child
        }
      }
    }
  }
}
