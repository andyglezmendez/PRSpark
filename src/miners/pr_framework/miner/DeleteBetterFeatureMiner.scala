package miners.pr_framework.miner

import PRFramework.Core.SupervisedClassifiers.DecisionTrees.Builder.DecisionTreeBuilder
import PRFramework.Core.SupervisedClassifiers.EmergingPatterns.{Miners, SubsetRelation}
import common.miner.{DatasetSchema, SparkMiner}
import common.pattern.ContrastPattern
import miners.pr_framework.SparkToPRFramework
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

class DeleteBetterFeatureMiner extends SparkMiner {
  override var dataMiner: DatasetSchema = _

  var MAX_DEPTH = 5
  var MIN_FEATURE_COUNT = 2
  var SUBSET_RELATION = DeleteBetterFeatureMiner.SUBSET_RELATION_EQUAL

  override def mine(rows: Array[Row]): Array[ContrastPattern] = {
    validateArguments()
    dataMiner = new DatasetSchema(rows)
    val wrapper = new SparkToPRFramework(rows, dataMiner)

    val miner = new Miners.DeleteBetterFeatureMiner
    val builder = new DecisionTreeBuilder
    builder.setMaxDepth(MAX_DEPTH)
    miner.setDecisionTreeBuilder(builder)
    miner.setFilterRelation(SUBSET_RELATION)
    miner.setMinFeatureCount(MIN_FEATURE_COUNT)

    val emergingPatterns = miner.mine(wrapper.model, wrapper.instances, wrapper.classFeature).asScala.toArray
    wrapper.emergingPatternToContrastPattern(emergingPatterns)
  }

  override protected def validateArguments(): Unit = {
    if (MAX_DEPTH < 0 || MAX_DEPTH > 99999)
      throw new IllegalArgumentException("MAX_DEPTH must be [0,99999]")
    if (MIN_FEATURE_COUNT < 0 || MIN_FEATURE_COUNT > 99999)
      throw new IllegalArgumentException("MIN_FEATURE_COUNT must be [0,99999]")
  }
}

object DeleteBetterFeatureMiner{
  val SUBSET_RELATION_SUPERSET = SubsetRelation.Superset
  val SUBSET_RELATION_EQUAL = SubsetRelation.Equal
}
