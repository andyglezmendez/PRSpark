package experiment.jep

import java.io.PrintWriter
import java.nio.file.{Files, Paths}

import common.dataframe.dicretizer.{EntropyBasedDiscretizer, IntegerToStringDiscretizer}
import common.dataframe.noiser.RandomNoise
import common.filter.{JEPFilter, MinimalPatternsFilter}
import common.miner.DatasetSchema
import common.model.Model
import common.pattern.ContrastPattern
import common.serialization.{ARFFSerializer, DirectoryTool}
import common.utils.ClassSelector
import experiment.{DataResult, ResultSerializer}
import miners.epm.miner.{BCEPMiner, SJEPMiner}
import miners.pr_framework.miner.{CEPMMiner, LCMiner, RandomForestMiner}
import models.{BCEPModel, iCAEPModel}
import org.apache.hadoop.mapred.FileAlreadyExistsException
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io


class PararellJEPExperiment extends ResultSerializer {
  val cores = 4
  var discretize = true
  var splitsCount = 10
  var processedDB = "D:\\Home\\School\\Tesis\\Results\\serialized_db\\discretized"
  var source = "D:\\Home\\School\\Tesis\\BD"
  var processedPatterns = "D:\\Home\\School\\Tesis\\Results\\serialized_patterns\\merged"
  var finalResults = "D:\\Home\\School\\Tesis\\Results\\serialized_results\\NO-JEP"
  var noiseDB = "D:\\Home\\School\\Tesis\\Results\\serialized_noise_db"
  var rData = "D:\\Home\\School\\Tesis\\Results\\spark\\new"

  var withMinedPatterns: Array[ContrastPattern] ⇒ Array[ContrastPattern] = patterns ⇒ patterns


  def serializeDB(): Unit = {
    val conf = new SparkConf().setMaster(s"local[$cores]")
      .setAppName("PR-Spark-DB-Process")
      .set("spark.sql.warehouse.dir", "..\\")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = SparkContext.getOrCreate()
    var dir = processedDB
    if (discretize) dir = dir + "\\discretized"

    //Load news DB URIs
    var filesPath = sc.parallelize(DirectoryTool.loadFiles(source, "*.arff"))
    //Load DB names previuosly serialized
    var serialized = DirectoryTool.loadFiles(dir, "*.arff").map(DirectoryTool.fileName)

    //Add files to Spark
    filesPath.foreach(file ⇒ SparkContext.getOrCreate().addFile(file))

    //Get new DB files name
    val newsDB = filesPath.collect().map(DirectoryTool.fileName) diff serialized
    val filesName = sc.parallelize(newsDB)

    import spark.implicits._
    //Load databases to Array of Row
    var loadedDF = filesName.map(file ⇒ {
      val serializer = new ARFFSerializer()
      (file, serializer.loadRowsFromARFF(SparkFiles.get(file)))
    })

    //Array of Row to Dataframe
    var dfs = loadedDF.collect().map(rows ⇒ {
      val df = spark.createDataFrame(sc.parallelize(rows._2), rows._2(0).schema)
      df.cache()
      (rows._1, df)
    })

    //Discretize the Dataframes
    if (discretize) {
      dfs = dfs.map(df ⇒ {
        val df1 = ClassSelector.selectClass(df._2, "class")
        val df2 = new EntropyBasedDiscretizer().discretize(df1)
        val df3 = new IntegerToStringDiscretizer().discretize(df2)
        df3.cache()
        try {
          df3.rdd.saveAsObjectFile(s"${dir}\\${df._1}")
        }
        catch {
          case _: FileAlreadyExistsException ⇒
            println(s"DATABASE => ${df._1} ALREADY EXISTS")
        }
        (df._1, df3)
      })
    }

  }

  def prepareCrossValidation(): Unit = {
    val conf = new SparkConf().setMaster(s"local[$cores]")
      .setAppName("PR-Spark-DB-Load")
      .set("spark.sql.warehouse.dir", "..\\")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = SparkContext.getOrCreate()

    //Load news DB URIs
    var filesPath = DirectoryTool.loadFiles(processedDB, "*.arff")
    val dfs: Array[(String, RDD[Row])] = filesPath.map(db ⇒ (DirectoryTool.fileName(db), sc.objectFile[Row](db)))
    //Define splits
    val splitSize = 1 / splitsCount.toDouble
    val splits = new Array[Double](splitsCount).map(zero ⇒ splitSize)
    val arrSplits = dfs.map(df ⇒ {
      val dfs1 = df._2.randomSplit(splits)
      (df._1, dfs1.map(_.collect()))
    })

    //Set partitions
    //        rddDF.repartition(4)

    //Cross-validation DataFrames distributions RDD[(Name, Training, Test)]
    val rddTraining = sc.parallelize(arrSplits).map(pair ⇒ {
      val arrDF = pair._2
      val trainingDFs = new Array[Array[Row]](arrDF.length)
      for (i ← arrDF.indices) {
        var training = Array.empty[Row]
        for (j ← arrDF.indices) {
          if (j != i) {
            if (training == null) training = arrDF(j)
            else training = training union arrDF(j)
          }
        }
        trainingDFs(i) = training
      }
      (pair._1, trainingDFs, arrDF)
    })

    rddTraining.saveAsObjectFile(s"${processedDB}\\all")
  }

  def makeNoise(noises: Array[Double]): Unit = {
    val conf = new SparkConf().setMaster(s"local[$cores]")
      .setAppName("PR-Spark-DB-Process")
      .set("spark.sql.warehouse.dir", "..\\")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = SparkContext.getOrCreate()

    //Load serialized databases
    val rddTraining: RDD[(String, Array[Array[Row]], Array[Array[Row]])] = sc.objectFile(processedDB + "\\all")
    rddTraining.cache()

    noises.foreach(noise ⇒ {
      val trainingNoise = rddTraining.map(training ⇒ {
        val noiseRows = training._2.map(arrRows ⇒ {
          RandomNoise.introduceNoise(arrRows, new DatasetSchema(arrRows), noise)
        });
        println(s"PROCESSED ${training._1} WITH NOISE ${noise}")
        (training._1, noiseRows, training._3)
      })
      println(s"END WITH NOISE ${noise}")
      trainingNoise.saveAsObjectFile(s"${noiseDB}\\noise-${noise}")
    })
  }

  def mineNoiseDB(loadFile: String, patternsFile: String, from: Int, end: Int): Unit = {
    val conf = new SparkConf().setMaster(s"local[10]")
      .setAppName("PR-Spark-DB-Process")
      .set("spark.sql.warehouse.dir", "..\\")
      .set("spark.executor.memory", "2g")
      .set("spark.driver.memory", "6g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = SparkContext.getOrCreate()

    //Load serialized databases
    var rddTraining: RDD[(String, Array[Array[Row]], Array[Array[Row]])] = sc.objectFile(noiseDB + s"\\${loadFile}")
    rddTraining = sc.parallelize(rddTraining.collect().slice(from, end))
    rddTraining.cache()

    //      rddTraining.repartition(20)
    //Mine
    val rddPatterns = rddTraining.map(training ⇒ {
      println(s"INIT DATABASE => ${training._1}")
      val minerResults = training._2.map(arrRow ⇒ {
        val miner = new RandomForestMiner()
        miner.MAX_DEPTH = 10
        //                miner.MIN_SUPPORT = 0.05
        var patterns = miner.mine(arrRow)
        //        patterns = new JEPFilter().filter(patterns)
        println(s"JEPS COUNT => ${patterns.length}")
        (patterns, arrRow, miner.dataMiner)
      })
      val result = new Array[(Array[ContrastPattern], Array[Row], DatasetSchema, Array[Row])](minerResults.length)
      for (i ← minerResults.indices) {
        result(i) = (minerResults(i)._1, minerResults(i)._2, minerResults(i)._3, training._3(i))
      }
      println(s"DATABASE => ${training._1}")
      println(s"COUNT RESULTS => ${minerResults.map(_._1.length).sum}")
      (training._1, result)
    })

    //Save Patterns
    rddPatterns.saveAsObjectFile(s"${processedPatterns}\\$patternsFile-no-jep")
  }

  def classifyNoisesDB(resultFile: String, models: Array[Model], dbs: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(s"local[1]")
      .setAppName("PR-Spark-DB-Process")
      .set("spark.sql.warehouse.dir", "..\\")
      .set("spark.executor.memory", "2g")
      .set("spark.driver.memory", "5g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = SparkContext.getOrCreate()


    for (elem <- models) {
      for (db ← dbs) {
        //Load Patterns
        val rddPatterns: RDD[(String, Array[(Array[ContrastPattern], Array[Row], DatasetSchema, Array[Row])])] =
          sc.objectFile(processedPatterns + s"\\${db}")
          rddPatterns.cache()

        //Classify
        val classification = rddPatterns.map(dbSet ⇒ {
          var rows = Array.empty[Row]
          var allPatternsLength = Array.empty[(Int, Int)]

          println(s"Begin Classified with DB ${dbSet._1}")
          var partition = 1
          val dbResults = dbSet._2.map(split ⇒ {
            val model = elem.getClass.newInstance()
            var patterns = split._1

            patterns = new JEPFilter().filter(patterns)
            println(s"Begin PARTITION ${partition} with DB ${dbSet._1}")
            val news = patterns.map(_.predicate.length)
              .groupBy(num ⇒ num)
              .map(pair ⇒ (pair._1, pair._2.length))
              .toArray

            news.foreach(pair ⇒ {
              val found = allPatternsLength.indexWhere(pair2 ⇒ pair._1 == pair2._1)
              if (found >= 0)
                allPatternsLength(found) = (allPatternsLength(found)._1, allPatternsLength(found)._2 + pair._2)
              else
                allPatternsLength = allPatternsLength :+ pair
            })

            println(s"Begin model PARTITION ${partition} DB ${dbSet._1}")
            model.initialize(patterns, split._2, split._3)
            rows = rows ++ split._4
            val splitResult = split._4.map(row ⇒ {
              val predictions = model.predict(row)
              //AQUI se calcula la precision por instancia(row)

              val classPrediction = model.getPredictionClass(predictions)
              (classPrediction.value, row.getString(row.size - 1))
            })
            println(s"End PARTITION ${partition} with DB ${dbSet._1}")
            partition += 1
            splitResult
          })

          println(s"End with DB ${dbSet._1} from $resultFile")

          (dbSet._1, dbResults.reduce(_ union _), allPatternsLength, new DatasetSchema(rows))
        })

        //Execute all
        //        classification.foreach(res ⇒ {
        //          println(s"DATABASE => ${res._1}")
        //          println(s"COUNT RESULTS => ${res._2.length}")
        //        })
        classification.saveAsObjectFile(s"$finalResults\\${elem.toString}-$resultFile-${db}")
      }

    }
  }

  def patternsLength(patterns: Array[ContrastPattern], oldLengths: Array[(Int, Int)]): Array[(Int, Int)] = {
    val news = patterns.map(_.predicate.length)
      .groupBy(num ⇒ num)
      .map(pair ⇒ (pair._1, pair._2.length))
      .toArray

    var lengths = oldLengths
    news.foreach(pair ⇒ {
      var found = lengths.indexWhere(pair2 ⇒ pair._1 == pair2._1)
      if (found >= 0)
        lengths(found) = (lengths(found)._1, lengths(found)._2 + pair._2)
      else
        lengths = lengths :+ pair
    })
    lengths
  }

  def toTabularView(): Unit = {
    val conf = new SparkConf().setMaster(s"local[$cores]")
      .setAppName("PR-Spark-DB-Process")
      .set("spark.sql.warehouse.dir", "..\\")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = SparkContext.getOrCreate()

    val results = DirectoryTool.loadFiles(finalResults, "*noise*")
    val fileNames = results.map(DirectoryTool.fileName).map(file ⇒ file.split("-"))

    for (result ← results) {
      val rddResult: RDD[(String, Array[(String, String)], Array[(Int,Int)] , DatasetSchema)] = sc.objectFile(result)
      val arrResults = rddResult.collect()
      val data = DirectoryTool.fileName(result).split("-")

      for (row ← arrResults) {
        val jepData = new JEPDataResult(row._1, data(5).toDouble, metric(row), row._3)
        val path = s"$rData\\${data(0)}-no-jep.data"
        saveData(path, jepData)
        println(s"Saved file in ${path}")
      }

    }
  }

  def metric(row: (String, Array[(String, String)], Array[(Int,Int)] , DatasetSchema)): Double = {
    var good = 0d
    row._2.foreach(pair ⇒ if (pair._1.equalsIgnoreCase(pair._2)) good += 1)
    good / row._2.length
  }
}
