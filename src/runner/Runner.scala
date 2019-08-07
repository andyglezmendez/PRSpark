package runner

import common.serialization.DirectoryTool
import experiment.accuracy.PararellAccuracyExperiment
import experiment.jep.JEPExperiment
import miners.epm.miner.{IEPMiner, SJEPMiner}
import models.{BCEPModel, iCAEPModel}

object Runner {
  def main(args: Array[String]): Unit = {
    acc_icaep()

  }

  def acc_icaep(): Unit = {
    val exp = new PararellAccuracyExperiment()
    exp.mineDB("autos.arff", "iep_miner")

//    exp.dataBases ++= DirectoryTool.loadFiles("resource\\db", "*.arff")
//    exp.classifiers ++= Array(new BCEPModel, new iCAEPModel)
//    val miner = new SJEPMiner()
//    miner.MIN_SUPPORT = 0.25
////    miner.MIN_GROWTH_RATE = 100000
//    exp.miner = miner
////    exp.noises ++= Array(0.15, 0.2, 0.22, 0.25)
//    exp.noises ++= Array(0.03, 0.05, 0.08, 0.1, 0.13, 0.15, 0.2, 0.22, 0.25)
//    exp.run()
  }

}
