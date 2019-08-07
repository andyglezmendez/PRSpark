package miners.pr_framework.miner

import PRFramework.Core.SupervisedClassifiers.DecisionTrees.Builder.DecisionTreeBuilder
import PRFramework.Core.SupervisedClassifiers.EmergingPatterns.{Miners, SubsetRelation}
import common.miner.{DatasetSchema, SparkMiner}
import common.pattern.ContrastPattern
import miners.pr_framework.SparkToPRFramework
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._


class RandomSplitMiner extends SparkMiner{
  override var dataMiner: DatasetSchema = _

  var MAX_DEPTH = 5
  var TREE_COUNT = 100
  var SUBSET_RELATION = RandomSplitMiner.SUBSET_RELATION_EQUAL
  var BEST_K_TO_SEARCH = 20

  override def mine(rows: Array[Row]): Array[ContrastPattern] = {
    validateArguments()
    dataMiner = new DatasetSchema(rows)
    val wrapper = new SparkToPRFramework(rows, dataMiner)

    val miner = new Miners.RandomSplitMiner
    val builder = new DecisionTreeBuilder
    builder.setMaxDepth(MAX_DEPTH)
    miner.setDecisionTreeBuilder(builder)
    miner.setFilterRelation(SUBSET_RELATION)
    miner.setTreeCount(TREE_COUNT)
    miner.setBestKToSearch(BEST_K_TO_SEARCH)

    val emergingPatterns = miner.mine(wrapper.model, wrapper.instances, wrapper.classFeature).asScala.toArray
    wrapper.emergingPatternToContrastPattern(emergingPatterns)
  }

  override protected def validateArguments(): Unit = {
    if (MAX_DEPTH < 0 || MAX_DEPTH > 99999)
      throw new IllegalArgumentException("MAX_DEPTH must be [0,99999]")
    if (TREE_COUNT < 1 || TREE_COUNT > 99999)
      throw new IllegalArgumentException("TREE_COUNT must be [0,99999]")
    if (BEST_K_TO_SEARCH < 0 || BEST_K_TO_SEARCH > 99999)
      throw new IllegalArgumentException("BEST_K_TO_SEARCH must be [0,99999]")
  }
}

object RandomSplitMiner{
  val SUBSET_RELATION_SUPERSET = SubsetRelation.Superset
  val SUBSET_RELATION_EQUAL = SubsetRelation.Equal
}
