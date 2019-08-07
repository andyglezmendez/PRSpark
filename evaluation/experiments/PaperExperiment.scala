package experiments

import common.serialization.DirectoryTool
import experiment.accuracy.AccuracyExperiment
import experiment.jep.JEPExperiment
import miners.epm.miner.{BCEPMiner, SJEPMiner}
import models.{BCEPModel, DTMModel, iCAEPModel}
import org.junit.Test

class PaperExperiment {

  @Test
  def accuracy_support(): Unit = {
    val exp = new AccuracyExperiment("D:\\Home\\School\\Data Mining\\Tesis\\Results\\spark")
    exp.dataBases ++= DirectoryTool.loadFiles("D:\\Home\\School\\Data Mining\\Tesis\\db", "*.arff")
    exp.classifiers ++= Array(new BCEPModel, new iCAEPModel, new DTMModel)
    exp.supports ++= Array(0.02, 0.06, 0.10, 0.15, 0.18, 0.20, 0.25, 0.30)
    exp.run()
  }


  @Test
  def jep_noise(): Unit = {
    val exp = new JEPExperiment("D:\\Home\\School\\Data Mining\\Tesis\\Results\\spark")
    exp.dataBases ++= DirectoryTool.loadFiles("D:\\Home\\School\\Data Mining\\Tesis\\db", "wine.arff")
    exp.classifiers ++= Array(new BCEPModel, new iCAEPModel)
    val miner = new SJEPMiner()
    miner.MIN_SUPPORT = 0.05
    exp.miner = miner
    exp.noises ++= Array(0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08)
    exp.run(5)
  }
}
