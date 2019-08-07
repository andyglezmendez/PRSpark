package miners.pr_spark.fp_max

import org.junit.Assert._
import org.junit.{Before, Test}

class FPTreeTestFPMax extends FPMaxUtilForTest {
  protected var fpTree: FPTree = _
  protected var node: FPNode = _
  protected var nodeOne: FPNode = _
  protected var nodeTwo: FPNode = _
  private var itemOne: Item = _
  private var itemTwo: Item = _
  protected var itemsetOne: Itemset = _
  protected var itemsetTwo: Itemset = _
  protected var arrItems: Array[Item] = _

  @Before
  def initFP(): Unit = {
    node = new FPNode(item)
    itemOne = new Item(2, 22, nominalFeatureTwo)
    itemTwo = new Item(3, 33, nominalFeatureThree)
    nodeOne = new FPNode(itemOne)
    nodeTwo = new FPNode(itemTwo)
    fpTree = new FPTree
    arrItems = Array(new Item(0, 12, nominalFeature), new Item(0, 22, nominalFeatureTwo), new Item(0, 12, nominalFeatureThree),
      new Item(1, 12, nominalFeature), new Item(1, 22, nominalFeatureTwo), new Item(1, 12, nominalFeatureThree), new Item(2, 12, nominalFeature),
      new Item(2, 22, nominalFeatureTwo), new Item(2, 12, nominalFeatureThree))
    itemset = new Itemset(Array(new Item(0, 12, nominalFeature), new Item(0, 22, nominalFeatureTwo), new Item(0, 12, nominalFeatureThree)))
    itemsetOne = new Itemset(Array(new Item(1, 12, nominalFeature), new Item(1, 22, nominalFeatureTwo), new Item(1, 12, nominalFeatureThree)))
    itemsetTwo = new Itemset(Array(new Item(2, 12, nominalFeature), new Item(2, 22, nominalFeatureTwo), new Item(2, 12, nominalFeatureThree)))

  }

  //FPNode Tests
  @Test
  def add_child_fpnode(): Unit = {
    node.addChild(nodeOne)
    assertEquals(node.childs.size, 1)
    assertNotNull(nodeOne.parent)
  }

  @Test
  def child_with_value_fpnode(): Unit = {
    node.addChild(nodeOne)
    assertNull(node.childWithValue(itemTwo))
    node.addChild(nodeTwo)
    assertNotNull(node.childWithValue(itemOne))
    assertNotNull(node.childWithValue(itemTwo))
  }


  //FPTree Tests
  @Test
  def build_header_table(): Unit = {
    fpTree.buildHeaderTable(Array(item, itemTwo, itemOne))
    val headerTable = fpTree.headerTable
    assertEquals(3, headerTable.size)
    assertNotNull(headerTable.find(_._1 == item).orNull)
    assertNotNull(headerTable.find(_._1 == itemOne).orNull)
    assertNotNull(headerTable.find(_._1 == itemTwo).orNull)
  }

  @Test
  def add_transaction(): Unit ={
    fpTree.buildHeaderTable(arrItems)
    fpTree.addTransaction(itemset)
    fpTree.addTransaction(itemsetTwo)
    fpTree.addTransaction(itemsetOne)
    assertEquals(9, fpTree.headerTable.size)
    assertTrue(fpTree.headerTable.values.forall(_ != null))
    assertNotNull(fpTree.headerTable(itemset.items(0)))
  }

//  @Test


}