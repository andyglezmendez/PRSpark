package experiment.accuracy

import common.dataframe.dicretizer.{EntropyBasedDiscretizer, IntegerToStringDiscretizer}
import common.dataframe.noiser.RandomNoise
import common.filter.{JEPFilter, MinimalPatternsFilter}
import common.miner.DatasetSchema
import common.model.Model
import common.pattern.ContrastPattern
import common.serialization.{ARFFSerializer, DirectoryTool}
import common.utils.ClassSelector
import experiment.ResultSerializer
import experiment.jep.JEPDataResult
import miners.epm.miner.IEPMiner
import miners.pr_framework.miner._
import org.apache.hadoop.mapred.FileAlreadyExistsException
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

class PararellAccuracyExperiment extends ResultSerializer {

  val cores = "*"
  var discretize = true
  var splitsCount = 10
  var processedDB = "C:\\Users\\Andy\\Development\\School\\CIARP\\serialized_db"
  var source = "D:\\Home\\School\\Tesis\\BD"
  var processedPatterns = "C:\\Users\\Andy\\Documents\\patterns"
  var finalResults = "D:\\Home\\School\\Tesis\\Results\\serialized_results\\ACC"
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
    else {
      dfs = dfs.map(df ⇒ {
        val df1 = ClassSelector.selectClass(df._2, "class")
        var df2 = df1
        if (df1.schema.fields.last.dataType == DoubleType)
          df2 = new EntropyBasedDiscretizer().discretize(df1, Array("class"))
        if (df1.schema.fields.last.dataType == IntegerType)
          df2 = new IntegerToStringDiscretizer().discretize(df1, Array("class"))
        df2.cache()
        try {
          df2.rdd.saveAsObjectFile(s"${dir}\\${df._1}")
        }
        catch {
          case _: FileAlreadyExistsException ⇒
            println(s"DATABASE => ${df._1} ALREADY EXISTS")
        }
        (df._1, df2)
      })
    }

  }

  def prepareCrossValidation(): Unit = {
    val conf = new SparkConf().setMaster(s"local[$cores]")
      .setAppName("PR-Spark-DB-Load")
      .set("spark.sql.warehouse.dir", "..\\")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = SparkContext.getOrCreate()
    var dir = processedDB
    if (discretize) dir = dir + "\\discretized"

    //Load news DB URIs
    var filesPath = DirectoryTool.loadFiles(dir, "*.arff")
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

    rddTraining.saveAsObjectFile(s"${dir}\\all")
  }

  def mineDB(loadDB: String, patternsFile: String): Unit = {
    val conf = new SparkConf().setMaster(s"local[$cores]")
      .setAppName("PR-Spark-DB-Process")
      .set("spark.sql.warehouse.dir", "..\\")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = SparkContext.getOrCreate()
    var dir = processedDB
    if (discretize) dir = dir + "\\discretized"

    //Load serialized databases
    val rddTraining: RDD[(String, Array[Array[Row]], Array[Array[Row]])] = sc.objectFile(dir + s"\\${loadDB}")
    rddTraining.cache()

    //Mine
    val rddPatterns = rddTraining.map(training ⇒ {
      val minerResults = training._2.map(arrRow ⇒ {
        val miner = new IEPMiner()
        var patterns = miner.mine(arrRow)

        println(s"Patterns COUNT => ${patterns.length}")
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

    rddPatterns.collect()
    //Save Patterns
//    rddPatterns.saveAsObjectFile(s"${processedPatterns}\\$patternsFile-accuracy")
  }

  def classify(resultFile: String, models: Array[Model], dbs: Array[String], support: Double): Unit = {
    val conf = new SparkConf().setMaster(s"local[*]")
      .setAppName("PR-Spark-DB-Process")
      .set("spark.sql.warehouse.dir", "..\\")
      .set("spark.executor.memory", "1g")
      .set("spark.driver.memory", "6g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = SparkContext.getOrCreate()

    for (elem <- models) {
      for (db ← dbs) {
        //Load Patterns
        val rddPatterns: RDD[(String, Array[(Array[ContrastPattern], Array[Row], DatasetSchema, Array[Row])])] =
          sc.objectFile(processedPatterns + s"\\${db}")

        //Classify
        val classification = rddPatterns.map(dbSet ⇒ {
          var rows = Array.empty[Row]
          var patternsCount = 0
          var partition: Int = 1
          val minimalData = new ArrayBuffer[Array[(Int, Int, Int, Int)]]()
          val dbResults = dbSet._2.map(split ⇒ {
            val model = elem.getClass.newInstance()
            val patternsSupport = split._1.filter(p ⇒ p.positiveClassSupport() >= support)
//            val minPatterns = MinimalPatternsFilter.filter(split._1)
//            minimalData += split._4.map(row ⇒ {
//              var good = 0
//              var bad = 0
//              minPatterns.foreach(cp ⇒ {
//                if (cp.predicate.forall(_.isMatch(row))) {
//                  if (cp.clazz.isMatch(row))
//                    good += 1
//                  else
//                    bad += 1
//                }
//              })
//              (partition, good, bad, minPatterns.length)
//            })

            partition += 1

            patternsCount += patternsSupport.length
            println(s"SUPPORT ${support} PATTERNS => ${patternsSupport.length}")
            if (patternsSupport.length == 0)
              println("MAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAALLLLLLLLL")

            model.initialize(patternsSupport, split._2, split._3)
            rows = rows ++ split._4
            val splitResult = split._4.map(row ⇒ {
              val predictions = model.predict(row)
              //AQUI se calcula la precision por instancia(row)

              val classPrediction = model.getPredictionClass(predictions)
              (classPrediction.value, row.getString(row.size - 1))
            })
            splitResult
          })

          /**
            * DB name
            * Array => (clasificacion, clase real)
            * Patterns Count
            * soporte
            * Array => (split, minimales clasifican bien, los mal, el total)
            * dataMiner general
            */
          (dbSet._1, dbResults.reduce(_ union _), patternsCount / partition, support, minimalData, new DatasetSchema(rows))
        })

        //Execute all
        classification.foreach(res ⇒ {
          println(s"DATABASE => ${res._1}")
          println(s"COUNT RESULTS => ${res._2.length}")
        })
        classification.saveAsObjectFile(s"$finalResults\\${elem.toString}-$resultFile-support-$support")
      }

    }
  }

  def toTabularView(): Unit = {
    val conf = new SparkConf().setMaster(s"local[$cores]")
      .setAppName("PR-Spark-DB-Process")
      .set("spark.sql.warehouse.dir", "..\\")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = SparkContext.getOrCreate()

    val results = DirectoryTool.loadFiles(finalResults, "iCAEP-acc-*")
    val fileNames = results.map(DirectoryTool.fileName).map(file ⇒ file.split("-"))

    for (result ← results) {
      val rddResult: RDD[(String, Array[(String, String)], Int, Double, Array[(Int, Int, Int, Int)], DatasetSchema)] = sc.objectFile(result)
      val arrResults = rddResult.collect()
      val data = DirectoryTool.fileName(result).split("-")

      for (row ← arrResults) {
        val accData = new AccuracyDataResult(row._1, data(3).toDouble, metric(row), row._3)
        val path = s"$rData\\${data(0)}-acc.data"
        saveData(path, accData)
        println(s"Saved file in ${path}")
      }

    }
  }

  def metric(row: (String, Array[(String, String)], Int, Double, Array[(Int, Int, Int, Int)], DatasetSchema)): Double = {
    var good = 0d
    row._2.foreach(pair ⇒ if (pair._1.equalsIgnoreCase(pair._2)) good += 1)
    good / row._2.length
  }

  def minimalClasification(min: Array[ContrastPattern], rows: Array[Row], partition: Int) = {
    rows.map(row ⇒ {
      var good = 0
      var bad = 0
      min.foreach(cp ⇒ {
        if (cp.predicate.forall(_.isMatch(row))) {
          if (cp.clazz.isMatch(row))
            good += 1
          else
            bad += 1
        }
      })
      (partition, good, bad, min.length)
    })
  }

}
