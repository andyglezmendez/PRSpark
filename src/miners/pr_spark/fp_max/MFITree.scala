package miners.pr_spark.fp_max

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


private[fp_max] class MFINode() extends Serializable {
  var value: Item = _
  var parent: MFINode = _
  var childs: ArrayBuffer[MFINode] = new ArrayBuffer[MFINode]()
  var level: Int = 0
  var nextHeader: MFINode = _

  def this(value: Item) {
    this()
    this.value = value
  }

  def addChild(child: MFINode): Unit = {
    child.parent = this
    child.level = level + 1
    childs += child
  }

  def childWithValue(item: Item): MFINode = {
    childs.find(_.value == item).orNull
  }

  override def toString: String = s"($level)$value"

}

private[fp_max] class MFITree extends Serializable {
  var root: MFINode = new MFINode()
  //Header Table that point to the first MFINode
  var headerTable: mutable.HashMap[Item, MFINode] = new mutable.HashMap[Item, MFINode]
  //Dictionary to the last MFINode pointer
  var lastNodePointer: mutable.HashMap[Item, MFINode] = new mutable.HashMap[Item, MFINode]

  var lastAddedNode: MFINode = _

  def addMFI(transaction: mutable.Iterable[Item]): Unit = {
    var currentNode = root
    for (item ← transaction) {
      var child = currentNode.childWithValue(item)

      if (child == null) {
        child = new MFINode(item)
        currentNode.addChild(child)
        fixNodeLinks(child)
      }
      currentNode = child
    }
    lastAddedNode = currentNode
  }

  def fixNodeLinks(newNode: MFINode): Unit = {
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

  def passSubsetChecking(itemset: List[Item]): Boolean = {
    if (lastAddedNode != null && isASubsetOfPrefixPath(itemset, lastAddedNode)) return false

    if (itemset.nonEmpty) {
      var tempNode = headerTable.get(itemset.last).orNull
      while (tempNode != null) {
        if (isASubsetOfPrefixPath(itemset, tempNode)) return false
        tempNode = tempNode.nextHeader
      }
    }
    true
  }

  def isASubsetOfPrefixPath(itemset: List[Item], node: MFINode): Boolean = {
    var testNode = node
    if (testNode.level >= itemset.size) {
      var itemCount = itemset.size
      do {
        if (itemset.contains(testNode.value)) {
          itemCount -= 1
          if (itemCount == 0) return true
        }
        testNode = testNode.parent
      } while (testNode != null)
    }
    false
  }

}
