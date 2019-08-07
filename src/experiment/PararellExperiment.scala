package experiment

import common.dataframe.dicretizer.{EntropyBasedDiscretizer, IntegerToStringDiscretizer}
import common.filter.JEPFilter
import common.miner.DatasetSchema
import common.pattern.ContrastPattern
import common.serialization.{ARFFSerializer, DirectoryTool}
import common.utils.ClassSelector
import miners.pr_framework.miner.RandomForestMiner
import models.{BCEPModel, iCAEPModel}
import org.apache.hadoop.mapred.FileAlreadyExistsException
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

class PararellExperiment extends ResultSerializer {
  val cores = 4
  var discretize = true
  var splitsCount = 10
  var processedDB = "D:\\Home\\School\\Tesis\\Results\\serialized_db"
  var source = "D:\\Home\\School\\Tesis\\BD"
  var processedPatterns = "D:\\Home\\School\\Tesis\\Results\\serialized_patterns"
  var finalResults = "D:\\Home\\School\\Tesis\\Results\\serialized_results"

  var withMinedPatterns: Array[ContrastPattern] ⇒ Array[ContrastPattern] = patterns ⇒ patterns


  def serializeDB(): Unit = {
    val conf = new SparkConf().setMaster(s"local[$cores]")
      .setAppName("PR-Spark-DB-Process")
      .set("spark.sql.warehouse.dir", "..\\")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = SparkContext.getOrCreate()

    //Load news DB URIs
    var filesPath = sc.parallelize(DirectoryTool.loadFiles(source, "*.arff"))
    //Load DB names previuosly serialized
    var serialized = DirectoryTool.loadFiles(processedDB, "*.arff").map(DirectoryTool.fileName)

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
          df3.rdd.saveAsObjectFile(s"D:\\Home\\School\\Tesis\\Results\\serialized_db\\${df._1}")
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

  def mine(patternsFile: String): Unit = {
    val conf = new SparkConf().setMaster(s"local[$cores]")
      .setAppName("PR-Spark-DB-Process")
      .set("spark.sql.warehouse.dir", "..\\")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = SparkContext.getOrCreate()

    //Load serialized databases
    val rddTraining: RDD[(String, Array[Array[Row]], Array[Array[Row]])] = sc.objectFile(processedDB + "\\all")
    rddTraining.cache()

    //Mine
    val rddPatterns = rddTraining.map(training ⇒ {
      val minerResults = training._2.map(arrRow ⇒ {
        val miner = new RandomForestMiner()
        //        miner.MIN_SUPPORT = 0.05
        var patterns = miner.mine(arrRow)
        patterns = new JEPFilter().filter(patterns)
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
    rddPatterns.saveAsObjectFile(s"${processedPatterns}\\$patternsFile")
  }

  def classify(resultFile: String): Unit = {
    val conf = new SparkConf().setMaster(s"local[$cores]")
      .setAppName("PR-Spark-DB-Process")
      .set("spark.sql.warehouse.dir", "..\\")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = SparkContext.getOrCreate()

    //Load Patterns
    val rddPatterns: RDD[(String, Array[(Array[ContrastPattern], Array[Row], DatasetSchema, Array[Row])])] = sc.objectFile(processedPatterns + "\\all")

    //Classify
    val classification = rddPatterns.map(dbSet ⇒ {
      var rows = Array.empty[Row]
      var patternsLength = 0
      val dbResults = dbSet._2.map(split ⇒ {
        val model = new iCAEPModel()
        model.initialize(split._1, split._2, split._3)
        rows = rows ++ split._4
        val splitResult = split._4.map(row ⇒ {
          val predictions = model.predict(row)
          //AQUI se calcula la precision por instancia(row)

          val classPrediction = model.getPredictionClass(predictions)
          (classPrediction.value, row.getString(row.size - 1))
        })
        if(split._1.length != new JEPFilter().filter(split._1).length)
          println("MAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAALLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLL")
        patternsLength = patternsLength + split._1.map(_.predicate.length).sum
        splitResult
      })
      val lengthAverage = patternsLength.toDouble / dbSet._2.map(pair ⇒ pair._1.length).sum
      (dbSet._1, dbResults.reduce(_ union _), lengthAverage, new DatasetSchema(rows))
    })

    //Execute all
    classification.foreach(res ⇒ {
      println(s"DATABASE => ${res._1}")
      println(s"COUNT RESULTS => ${res._2.length}")
    })
    classification.saveAsObjectFile(s"$finalResults\\$resultFile")
  }

}
