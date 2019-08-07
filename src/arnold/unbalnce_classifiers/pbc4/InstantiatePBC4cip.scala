package classifiers.unbalnce_classifiers.pbc4

import common.pattern.ContrastPattern
import miners.pr_framework.miner.RandomForestMiner
import org.apache.spark.sql.Row

object InstantiatePBC4cip {
  def buildClassifierWithRows(rows: Array[Row]): PBC4cip = {
    var result = new PBC4cip();
    {
      var randomForestMiner = new RandomForestMiner;

      //Poner metrica de hellinger

      var minedPatterns = randomForestMiner.mine(rows);
      result.initializeClassifier(minedPatterns);
    }
    return result;
  }

  def buildClassifierWithContrastPatterns(patterns: Array[ContrastPattern]): PBC4cip = {
    var result = new PBC4cip();
    {
      result.initializeClassifier(patterns);
    }
    return result;
  }
}
