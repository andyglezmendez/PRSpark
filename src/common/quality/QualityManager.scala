package common.quality

import common.pattern.ContrastPattern
import common.quality.qualities._

class QualityManager(patternCollection: Array[ContrastPattern]) extends Serializable {

  /**
    * Default instances of the qualities
    * here goes all the registered qualities
    */
  val qualitiesFunctionArray = Array(
    new AccuracyQuality(),
    new BrinsQuality(),
    new C1Quality,
    new C2Quality,
    new CausalConfidenceQuality,
    new CausalConfirmedConfidenceQuality,
    new CausalConfirmQuality,
    new CenteredConfidence,
    // new ChiSquareQuality,
    new CohenQuality,
    new ColemanQuality(),
    new CollectiveStrengthQuality,
    new ConfidenceQuality,
    new ConfidenceYatesCorrectionQuality,
    new CosineQuality,
    new CoverageQuality,
    new DependencyQuality,
    new DescriptiveConfirmConfQuality,
    new DescriptiveConfirmQuality,
    new ExampleCounterExampleRateQuality,
    // new FisherPlusChiSquareQuality,
    //  new G2Quality,
    new GainQuality,
    new GiniIndexQuality,
    new GrowthRateQuality,
    new ImplicationIndexQuality,
    new InformationGainQuality,
    new JaccardIndexQuality,
    new JMeassureQuality,
    new KlosgenQuality,
    new LaplaceCorrectionQuality,
    new LeastContradictionQuality,
    new LermanQuality,
    new LeverageQuality,
    new LiftQuality,
    new MeasureOfDiscriminationQuality,
    new MutualInformationQuality,
    new NetConfQuality,
    new OddsRatioQuality,
    new PearsonCorrelationQuality,
    new PrevalecenceQuality,
    new ProductOfConfidenceAndCoverageQuality,
    new PutativeCausalDependencyQuality,
    new RelativeRiskQuality,
    new RuleInterestQuality,
    new SebagSchoenauerQuality,
    new SpecificityQuality,
    new StrengthQuality,
    new SupportDifferenceQuality,
    new WeightedSumConfidenceAndCoverageQuality,
    new WraccQuality,
    new YaoLiuOneWaySupportQuality,
    new YaoLiuTwoWaySupportQuality,
    new YulesQQuality,
    new YulesYQuality,
    new ZhangQuality,
    new MultiplyQuality(null, null)
  );

  /**
    * This is the list of registered qualities that will be applied to the pattern
    */
  var registerQuality = new Array[BaseQuality](qualitiesFunctionArray.length)

  var count = 0;

  def addAllQualities(): Unit = {
    var i = 0;
    while (i < qualitiesFunctionArray.length) {
      addQuality(qualitiesFunctionArray(i))
      i += 1
    }
  }

  def addQualities(names: Array[String]): Unit = {
    var i = 0;
    while (i < names.length) {
      addQuality(names(i))
      i += 1
    }
  }

  def addQuality(name: String): Unit = {
    val index = qualityIndex(name)
    addQuality(qualitiesFunctionArray(index))
  }

  def addQuality(quality: BaseQuality): Unit = {
    registerQuality.update(count, quality)
    count += 1
  }

  def cleanAll(): Unit = {
    count = 0;
  }

  private def qualityIndex(name: String): Int = {
    var result = -1;
    {
      var i = 0;
      var found = false
      while (i < qualitiesFunctionArray.length && !found) {
        val quality = qualitiesFunctionArray(i)
        if (quality.name.equals(name)) {
          result = i
          found = true
        }
        i += 1
      }
    }
    return result;
  }

  private def calculateQualityValues(contrastPattern: ContrastPattern): Array[Double] = {
    val result = new Array[Double](count);
    {
      var i = 0;
      while (i < result.length) {
        val q = registerQuality(i)
        result(i) = q.getQuality(contrastPattern)

        i += 1
      }
    }
    return result
  }


  def calcMetrics(): Unit = {
    var i = 0;
    while (i < patternCollection.length) {

      val pattern = patternCollection(i)

      val patternQualities = calculateQualityValues(pattern)

      var j = 0;
      while (j < patternQualities.length) {
        val q = registerQuality(j)

        pattern.metricMap.put(q.name, patternQualities(j))

        j += 1
      }
      i += 1
    }
  }
}

