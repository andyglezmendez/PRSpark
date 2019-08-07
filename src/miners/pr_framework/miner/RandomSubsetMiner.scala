package miners.pr_framework.miner

import PRFramework.Core.SupervisedClassifiers.DecisionTrees.Builder.DecisionTreeBuilder
import PRFramework.Core.SupervisedClassifiers.EmergingPatterns.{Miners, SubsetRelation}
import common.miner.{DatasetSchema, SparkMiner}
import common.pattern.ContrastPattern
import miners.pr_framework.SparkToPRFramework
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

class RandomSubsetMiner extends SparkMiner{
  override var dataMiner: DatasetSchema = _

  var MAX_DEPTH = 5
  var TREE_COUNT = 100
  var SUBSET_SIZE = 2
  var SUBSET_PERCENT = 20
  var SUBSET_RELATION = RandomSubsetMiner.SUBSET_RELATION_EQUAL

  override def mine(rows: Array[Row]): Array[ContrastPattern] = {
    validateArguments()
    dataMiner = new DatasetSchema(rows)
    val wrapper = new SparkToPRFramework(rows, dataMiner)

    val miner = new Miners.RandomFeatureSubsetMiner
    val builder = new DecisionTreeBuilder
    builder.setMaxDepth(MAX_DEPTH)
    miner.setDecisionTreeBuilder(builder)
    miner.setFilterRelation(SubsetRelation.Equal)
    miner.setTreeCount(TREE_COUNT)
    miner.setSubsetSize(SUBSET_SIZE)
    miner.setSubsetPercent(SUBSET_PERCENT)

    val emergingPatterns = miner.mine(wrapper.model, wrapper.instances, wrapper.classFeature).asScala.toArray
    wrapper.emergingPatternToContrastPattern(emergingPatterns)
  }

  override protected def validateArguments(): Unit = {
    if (MAX_DEPTH < 0 || MAX_DEPTH > 99999)
      throw new IllegalArgumentException("MAX_DEPTH must be [0,99999]")
    if (TREE_COUNT < 0 || TREE_COUNT > 99999)
      throw new IllegalArgumentException("TREE_COUNT must be [0,99999]")
    if (SUBSET_SIZE < 0 || SUBSET_SIZE > 99999)
      throw new IllegalArgumentException("SUBSET_SIZE must be [0,99999]")
    if (SUBSET_PERCENT < 0 || SUBSET_PERCENT > 99999)
      throw new IllegalArgumentException("SUBSET_PERCENT must be [0,99999]")
  }
}

object RandomSubsetMiner{
  val SUBSET_RELATION_SUPERSET = SubsetRelation.Superset
  val SUBSET_RELATION_EQUAL = SubsetRelation.Equal
}