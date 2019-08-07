package miners.pr_framework.miner

import PRFramework.Core.SupervisedClassifiers.DecisionTrees.Builder.DecisionTreeBuilder
import PRFramework.Core.SupervisedClassifiers.DecisionTrees.DistributionTesters.PureNodeStopCondition
import PRFramework.Core.SupervisedClassifiers.DecisionTrees.PruneTesters.PessimisticError
import PRFramework.Core.SupervisedClassifiers.EmergingPatterns.PatternTests.QualityBasedPatternTester
import PRFramework.Core.SupervisedClassifiers.EmergingPatterns.Qualities.Statistical.GrowthRateQuality
import PRFramework.Core.SupervisedClassifiers.EmergingPatterns.{Miners, PatternTestHelper, SubsetRelation}
import common.miner.{DatasetSchema, SparkMiner}
import common.pattern.ContrastPattern
import miners.pr_framework.SparkToPRFramework
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

class CEPMMiner extends SparkMiner {
  override var dataMiner: DatasetSchema = _

  var MAX_DEPTH = 5
  var MAX_ITERATIONS = 20
  var PRUNE_RESULT = true
  var GROWTH_RATE = 100.0
  var SUBSET_RELATION = RandomForestMiner.SUBSET_RELATION_EQUAL

  override def mine(rows: Array[Row]): Array[ContrastPattern] = {
    validateArguments()
    dataMiner = new DatasetSchema(rows)
    val wrapper = new SparkToPRFramework(rows, dataMiner)

    val miner = new Miners.CepmMiner
    val builder = new DecisionTreeBuilder
    builder.setMaxDepth(MAX_DEPTH)
    builder.setPruneResult(PRUNE_RESULT)
    builder.setPruneTester(new PessimisticError)
    builder.setMinimalInstanceMembership(0.05)
    builder.setStopCondition(new PureNodeStopCondition)


    miner.setEPTester(new QualityBasedPatternTester(new GrowthRateQuality, GROWTH_RATE))
    miner.setDecisionTreeBuilder(builder)
    miner.setFilterRelation(SUBSET_RELATION)
    miner.setMaxIterations(MAX_ITERATIONS)

    val emergingPatterns = miner.mine(wrapper.model, wrapper.instances, wrapper.classFeature).asScala.toArray
    wrapper.emergingPatternToContrastPattern(emergingPatterns)
  }

  override protected def validateArguments(): Unit = {
    if (MAX_DEPTH < 0 || MAX_DEPTH > 99999)
      throw new IllegalArgumentException("MAX_DEPTH must be [0,99999]")
    if (MAX_ITERATIONS < 0 || MAX_ITERATIONS > 99999)
      throw new IllegalArgumentException("MAX_ITERATIONS must be [0,99999]")
  }
}

object CEPMMiner {
  val SUBSET_RELATION_DIFFERENT = SubsetRelation.Different
  val SUBSET_RELATION_EQUAL = SubsetRelation.Equal
}