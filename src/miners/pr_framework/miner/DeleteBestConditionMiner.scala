package miners.pr_framework.miner

import PRFramework.Core.SupervisedClassifiers.DecisionTrees.Builder.DecisionTreeBuilder
import PRFramework.Core.SupervisedClassifiers.EmergingPatterns.{Miners, SubsetRelation}
import common.miner.{DatasetSchema, SparkMiner}
import common.pattern.ContrastPattern
import miners.pr_framework.SparkToPRFramework
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

class DeleteBestConditionMiner extends SparkMiner{
  override var dataMiner: DatasetSchema = _

  var MAX_DEPTH = 5
  var MAX_TREE = 20
  var LAST_MINING_ITERATIONS = 20
  var SUBSET_RELATION = DeleteBestConditionMiner.SUBSET_RELATION_EQUAL

  override def mine(rows: Array[Row]): Array[ContrastPattern] = {
    validateArguments()
    dataMiner = new DatasetSchema(rows)
    val wrapper = new SparkToPRFramework(rows, dataMiner)

    val miner = new Miners.DeleteBestConditionMiner
    val builder = new DecisionTreeBuilder
    builder.setMaxDepth(MAX_DEPTH)
    miner.setDecisionTreeBuilder(builder)
    miner.setFilterRelation(SUBSET_RELATION)
    miner.setMaxTree(MAX_TREE)
    miner.setLastMiningIterations(LAST_MINING_ITERATIONS)

    val emergingPatterns = miner.mine(wrapper.model, wrapper.instances, wrapper.classFeature).asScala.toArray
    wrapper.emergingPatternToContrastPattern(emergingPatterns)
  }

  override protected def validateArguments(): Unit = {
    if (MAX_DEPTH < 0 || MAX_DEPTH > 99999)
      throw new IllegalArgumentException("MAX_DEPTH must be [0,99999]")
    if (MAX_TREE < 0 || MAX_TREE > 99999)
      throw new IllegalArgumentException("MAX_TREE must be [0,99999]")
    if (LAST_MINING_ITERATIONS < 1 || LAST_MINING_ITERATIONS > 99999)
      throw new IllegalArgumentException("LAST_MINING_ITERATIONS must be [1,99999]")
  }
}

object DeleteBestConditionMiner{
  val SUBSET_RELATION_SUPERSET = SubsetRelation.Superset
  val SUBSET_RELATION_EQUAL = SubsetRelation.Equal
}
