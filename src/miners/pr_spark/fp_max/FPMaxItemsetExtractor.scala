package miners.pr_spark.fp_max

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

private[fp_max] class FPMaxItemsetExtractor extends Serializable {

  var itemsetPatterns: ArrayBuffer[Itemset] = _
  private var _minimalCount: Int = 0
  private var _mfiTree: MFITree = _
  private var _prefixs: Array[Item] = _

  def extractMaximaItemsets(frequentItems: mutable.Iterable[Item], transactions: mutable.Iterable[Itemset], minSupport: Int): ArrayBuffer[Itemset] = {
    itemsetPatterns = new ArrayBuffer[Itemset]()
    _minimalCount = minSupport
    _prefixs = new Array[Item](frequentItems.size)

    val itm = frequentItems.filter(_.support >= minSupport)

//    val tree = buildFPTree(frequentItems, transactions)
    val tree = buildFPTree(itm, transactions)
    _mfiTree = buildMFITree(tree.headerTable.keys)

//    val supports = tree.headerTable.keys.filter(_.support >= minSupport).map(item ⇒ (item, item.support)).toMap
    val supports = tree.headerTable.keys.map(item ⇒ (item, item.support)).toMap

    if (tree.root.childs.nonEmpty) fpMax(tree, _prefixs, 0, transactions.size, supports)
    itemsetPatterns
  }

  def isSinglePath(root: FPNode, prefixs: Array[Item], length: Int, minCount: Int): Boolean = {
    var prefixsLength = length
    val singlePathItemset = prefixs
    var singlePathSupport = 0
    var singlePath = true
    if (root.childs.size > 1) singlePath = false
    else {
      var node = root.childs.head
      var endWhile = false
      while (!endWhile) {
        if (node.childs.size > 1) {
          singlePath = false
          endWhile = true
        }
        if (singlePath) {
          singlePathItemset(prefixsLength) = node.value
          singlePathSupport = node.counter
          if (node.childs.isEmpty) {
            endWhile = true
          }
          else {
            prefixsLength += 1
            node = node.childs.head
          }
        }
      }
    }
    if (singlePath && singlePathSupport >= minCount)
      saveItemset(singlePathItemset, prefixsLength, singlePathSupport)
    singlePath
  }

  def fpMax(tree: FPTree, prefixs: Array[Item], prefixsLength: Int, prefixSupport: Int, supports: Map[Item, Double]): Unit = {
    if (!isSinglePath(tree.root, prefixs, prefixsLength, _minimalCount)) {

      val inverseTableHeader = tree.headerTable.keys.toArray.sortBy(item ⇒ item.feature.index).reverse
      for (item ← inverseTableHeader) {
        val support = supports(item)
        prefixs(prefixsLength) = item
        val betaSupport = if (prefixSupport < support) prefixSupport else support

        var prefixsPaths = new ArrayBuffer[ArrayBuffer[FPNode]]()
        var path = tree.headerTable(item)

        var betaSupports = new mutable.HashMap[Item, Double]()

        while (path != null) {
          if (path.parent.value != null) {
            var prefixPath = new ArrayBuffer[FPNode]()
            prefixPath += path
            val pathCounter = path.counter
            var parent = path.parent

            while (parent.value != null) {
              prefixPath += parent
              if (!betaSupports.contains(parent.value))
                betaSupports.+=((parent.value, pathCounter))
              else
                betaSupports.+=((parent.value, betaSupports(parent.value) + pathCounter))
              parent = parent.parent
            }
            prefixsPaths += prefixPath
          }
          path = path.nextHeader
        }
        var headWithP = copyItemsUntil(prefixs, prefixsLength)
        betaSupports.filter(_._2 >= _minimalCount).keys.foreach(item ⇒ headWithP += item)

        headWithP = headWithP.sortBy(item ⇒ item.support).reverse
        if (_mfiTree.passSubsetChecking(headWithP.toList)) {
          val betaTree = new FPTree
          betaTree.addPrefixPaths(prefixsPaths, betaSupports, _minimalCount)
          if (betaTree.root.childs.nonEmpty)
            fpMax(betaTree, prefixs, prefixsLength + 1, betaSupport.toInt, betaSupports.toMap)

          val evaluation = copyItemsUntil(prefixs, prefixsLength)
          if (_mfiTree.passSubsetChecking(evaluation.toList))
            saveItemset(prefixs, prefixsLength, betaSupport)
        }
      }
    }
  }

  def saveItemset(itemset: Array[Item], itemsetLength: Int, support: Double): Unit = {
    val tempItemset = copyItemsUntil(itemset, itemsetLength).sortBy(item ⇒ item.support).reverse
    _mfiTree.addMFI(tempItemset)
    if (tempItemset.nonEmpty)
      itemsetPatterns += new Itemset(tempItemset)
  }

  def copyItemsUntil(prefixs: Array[Item], prefixsLength: Int): ListBuffer[Item] = {
    val result = new ListBuffer[Item]
    result ++ prefixs.slice(0, prefixsLength+1).sortBy(item ⇒ item.support).reverse
  }


  def buildFPTree(frequentItems: Iterable[Item], transactions: Iterable[Itemset]): FPTree = {
    val fpTree = new FPTree
    fpTree.buildHeaderTable(frequentItems)
    fpTree.addMultipleTransactions(transactions)
    fpTree
  }

  def buildMFITree(frequentItems: Iterable[Item]): MFITree = {
    _mfiTree = new MFITree
    _mfiTree.buildHeaderTable(frequentItems)
    _mfiTree
  }


}
