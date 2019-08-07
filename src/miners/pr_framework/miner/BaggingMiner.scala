package miners.pr_framework.miner

import PRFramework.Core.SupervisedClassifiers.DecisionTrees.Builder.DecisionTreeBuilder
import PRFramework.Core.SupervisedClassifiers.EmergingPatterns.{Miners, SubsetRelation}
import common.miner.{DatasetSchema, SparkMiner}
import common.pattern.ContrastPattern
import miners.pr_framework.SparkToPRFramework
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

class BaggingMiner extends SparkMiner{
  override var dataMiner: DatasetSchema = _

  var CONFIDENCE = 0.6
  var MIN_GROWTH_RATE = 10.0
  var MAX_DEPTH = 5
  var TREE_COUNT = 20
  var SUBSET_RELATION = BaggingMiner.SUBSET_RELATION_SUPERSET

  override def mine(rows: Array[Row]): Array[ContrastPattern] = {
    validateArguments()
    dataMiner = new DatasetSchema(rows)
    val wrapper = new SparkToPRFramework(rows, dataMiner)

    val miner = new Miners.BaggingMiner //Can mine patterns without predicate
    val builder = new DecisionTreeBuilder
    builder.setMaxDepth(MAX_DEPTH)

    miner.setDecisionTreeBuilder(builder)
    miner.setFilterRelation(SUBSET_RELATION)
    miner.setTreeCount(TREE_COUNT)

    val emergingPatterns = miner.mine(wrapper.model, wrapper.instances, wrapper.classFeature).asScala.toArray
    wrapper.emergingPatternToContrastPattern(emergingPatterns)
  }

  override protected def validateArguments(): Unit = {
    if (CONFIDENCE < 0 || CONFIDENCE > 1)
      throw new IllegalArgumentException("CONFIDENCE must be [0,1]")
    if (MIN_GROWTH_RATE < 1 || MIN_GROWTH_RATE > 100000)
      throw new IllegalArgumentException("MIN_GROWTH_RATE must be [1,100000]")
    if (MAX_DEPTH < 0 || MAX_DEPTH > 99999)
      throw new IllegalArgumentException("MAX_DEPTH must be [0,99999]")
    if (TREE_COUNT < 0 || TREE_COUNT > 99999)
      throw new IllegalArgumentException("TREE_COUNT must be [0,99999]")
  }
}

object BaggingMiner{
  val SUBSET_RELATION_SUPERSET = SubsetRelation.Superset
  val SUBSET_RELATION_EQUAL = SubsetRelation.Equal
}