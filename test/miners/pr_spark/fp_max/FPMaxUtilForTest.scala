package miners.pr_spark.fp_max

import PRFramework.Core.Common.NominalFeature
import common.feature.CategoricalFeature
import org.junit.Before

import scala.collection.mutable.ListBuffer

class FPMaxUtilForTest {
  protected var item: Item = _
  protected var nominalFeature: CategoricalFeature = _
  protected var nominalFeatureTwo: CategoricalFeature = _
  protected var nominalFeatureThree: CategoricalFeature = _
  protected var itemset: Itemset = _

  @Before
  def init(): Unit = {
    nominalFeature = new CategoricalFeature("Feature", 0, "")

    nominalFeatureTwo = new CategoricalFeature("FeatureTwo", 1,"")

    nominalFeatureThree = new CategoricalFeature("FeatureThree", 2,"")

    item = new Item(value = 1.0, support = 1.0, nominalFeature)

    val items = new ListBuffer[Item]
    items += item
    itemset = new Itemset(items)
  }

}
