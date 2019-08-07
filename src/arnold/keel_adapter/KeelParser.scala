package arnold.keel_adapter

import scala.io.Source
import java.io.PrintWriter

import org.apache.hadoop.hive.metastore.api.InvalidOperationException
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * This class parse a file of the keel repository
  *
  * @param filePath The path of the File
  */
class KeelParser(filePath: String) {

  /**
    * The name of the keel database
    */
  private var relationName = ""

  /**
    * The attributes that contains the keel database
    */
  private var attributes: ListBuffer[KeelToken] = new ListBuffer[KeelToken]

  /**
    * The data stored on the keel database
    */
  private var data: ListBuffer[String] = new ListBuffer[String]

  /**
    * This attribute indicates if the database was parsed
    */
  private var databaseParsed = false

  /**
    * This method parse the database
    */
  def parseDatabase(): Unit = {

    val file = Source.fromFile(filePath)
    var takeData = false
    if (file.isEmpty)
      return
    for (originalLine: String <- file.getLines()) {
      var line = originalLine

      if (line.contains("@relation"))
        this.relationName = line.replace("@relation", "").trim
      else if (line.contains("@attribute")) {
        val cleanLine = line.replace("@attribute", "").trim
        attributes += KeelToken.parseLine(cleanLine)
      }
      else if (line.contains("@inputs")) {
        line = line.replace("@inputs", "")
        line = removeBlankSpaces(line)
        val inputsFeature = line.split(",")


        for (in <- inputsFeature) {
          var i = 0
          var finish = false
          while (i < attributes.length && !finish) {
            val token = attributes(i)
            if (token.featureName != null && token.featureName.equals(in)) {
              token.isInput = true;
              finish = true
            }
            i += 1
          }
        }
      }
      else if (line.contains("@outputs")) {
        line = line.replace("@outputs", "")
        line = removeBlankSpaces(line)
        val outputFeature = line.split(",")

        for (in <- outputFeature) {
          var i = 0
          var finish = false
          while (i < attributes.length && !finish) {
            val token = attributes(i)
            if (token.featureName != null && token.featureName.equals(in)) {
              token.isOutput = true;
              finish = true
            }
            i += 1
          }
        }
      }
      else if (line.contains("@data"))
        takeData = true
      else if (takeData)
        data += removeBlankSpaces(line)
    }
    file.close()
    databaseParsed = true;
  }

  /**
    * This method must be call after the method "parseDatabase", the database call this method to obtain a
    * List Buffer that will contains the rows of the original database
    *
    * @return A ListBuffer[Row] containing the rows. Every row have his schema defined.
    */
  def getRowList(): ListBuffer[Row] = {
    val result: ListBuffer[Row] = new ListBuffer[Row];
    {
      if (!this.databaseParsed)
        throw new InvalidOperationException("The method 'parseDatabase' must be called first")

      val schema = buildSchemaOfTheRows()
      for (row <- this.data) {
        val rowValues = buildRow(row, schema)
        val rowRow: Row = new GenericRowWithSchema(rowValues, schema)
        result += rowRow
      }
    }
    return result;
  }

  /**
    * This method build the schema of the rows
    *
    * @return the schema associated to the selected database
    */
  private def buildSchemaOfTheRows(): StructType = {
    val arrayStructTypes: ListBuffer[StructField] = new ListBuffer[StructField];

    for (attribute <- this.attributes) {
      val name = attribute.featureName
      val typeField = getFeatureType(attribute)
      val structField = new StructField(name, typeField, false)

      //schema.add(structField)
      arrayStructTypes += structField

    }
    return new StructType(arrayStructTypes.toArray);
  }

  /**
    * This method match the type of the keel repository with the programming type. . This method can suffer chains if other data types are aggregated
    * Example "real" = "DoubleType"
    * Example "Class" = "StringType"
    *
    * @param token the token that we want analise
    * @return The current type of the token.
    */
  private def getFeatureType(token: KeelToken): DataType = {

    var result = numericalFeature(token)
    if (result == null)
      result = categoricalFeature(token)

    if (result == null)
      throw new InvalidOperationException("Type not supported")

    return result

    /*
    if (token.isOutput)
      return StringType
    else if (token.featureType.equals("real"))
      return DoubleType
    //To add a new data type affect the code here...
    else throw new InvalidOperationException("Type not supported")
    */
  }

  private def categoricalFeature(token: KeelToken): DataType = {
    if (token.featureType == null) {
      return StringType

      //To add a new data type affect the code here...
    }

    return null
  }

  private def numericalFeature(token: KeelToken): DataType = {
    if (token.featureType != null) {
      if (token.featureType.equals("real"))
        return DoubleType
      else if (token.featureType.equals("integer"))
        return IntegerType;

      //To add a new data type affect the code here...
    }
    return null
  }

  /**
    * This method build the row. This method can suffer chains if other data types are aggregated
    *
    * @param rowStr the row on the String format that we want parse
    * @param schema the schema corresponding to this database
    * @return An Array[Any] with the data parsed
    */
  private def buildRow(rowStr: String, schema: StructType): Array[Any] = {

    val valuesStr = rowStr.split(",")
    for (i <- 0 until valuesStr.length) {
      valuesStr(i) = valuesStr(i).trim
    }

    val result: Array[Any] = new Array[Any](valuesStr.length);
    {
      for (i <- 0 until valuesStr.length) {
        //To add a new data value type affect the code here
        if (schema.fields(i).dataType == DoubleType)
          result(i) = valuesStr(i).toDouble
        else if (schema.fields(i).dataType == StringType)
          result(i) = valuesStr(i)

        //To add a new data value type affect the code here...
      }
    }
    return result
  }

  /**
    * The name of the keel database
    */
  def getRelationName(): String = {
    if (databaseParsed)
      return this.relationName
    else throw new InvalidOperationException("First call the method 'parseDatabase'")
  }

  /**
    * The attributes that contains the keel database
    */
  def getAttributes(): ListBuffer[KeelToken] = {
    if (databaseParsed)
      return this.attributes
    else throw new InvalidOperationException("First call the method 'parseDatabase'")
  }

  /**
    * The data stored on the keel database
    */
  def getData(): ListBuffer[String] = {
    if (databaseParsed)
      return this.data
    else throw new InvalidOperationException("First call the method 'parseDatabase'")
  }


  /**
    * Generates a file in csv format. The file must be a file downloaded from the KEEL repository.
    * The path of the generated is the root of the project
    *
    * @param fileName The name of the file that will be generated
    */
  def convertFileToCSV(fileName: String): Boolean = {
    var result = false;
    {
      try {
        var header: String = null;
        var copyData = false
        var existOutputAnnotation = false
        var parseAttributes = false

        var ho = "";
        val outputFile = new PrintWriter(fileName)

        val file = Source.fromFile(filePath)
        for (originalLine <- file.getLines()) {

          val line = removeBlankSpaces(originalLine)
          if (line.contains("@attribute") || line.contains("attribute")) {
            header = prepareHeader(line)
            header = cleanDataType(header)
            parseAttributes = true
            ho = ho + header + ","
          }
          else if (line.contains("@inputs")) {
            if (!parseAttributes) {
              header = prepareHeader(line)
              header = cleanDataType(header)
              ho = ho + header + ","
            }
          }
          else if (line.contains(("@outputs"))) {
            val h1 = prepareOutput(line)
            if (!parseAttributes)
              ho += h1
            else ho = ho.substring(0, ho.length - 1)
            outputFile.println(ho)
            existOutputAnnotation = true
          }
          else if (line.contains("@data")) {
            copyData = true
            if (!existOutputAnnotation) {
              ho = ho.substring(0, ho.length - 1)
              outputFile.println(ho)
            }

          }

          else if (copyData) {
            outputFile.println(line.trim)
          }
        }
        outputFile.flush()
        outputFile.close()
        file.close()

        result = true
      }
      catch {
        case e: Exception => {
          result = false
        }
      }
    }
    return result
  }

  /**
    * This method prepares the header of the csv file
    *
    * @param line line that contains the begining of the header
    * @return the first part of the header without any blank space
    */
  private def prepareHeader(line: String): String = {
    var change1 = line.replace("@inputs", "").trim
    change1 = change1.replace("@attribute", "").trim
    change1 = change1.replace("attribute", "").trim
    return change1.replace("inputs", "").trim
  }

  def cleanDataType(header: String): String = {
    var change1 = header.replace("real", "")
    change1 = change1.replace("integer", "")
    if (change1.contains("{"))
      change1 = change1.substring(0, change1.indexOf('{'))
    else if (change1.contains("["))
      change1 = change1.substring(0, change1.indexOf('['))
    return change1
  }

  /**
    * This method ends the preparation of the header of the csv file
    *
    * @param line line that contains the end of the header
    * @return the end of the header without any blank space
    */
  private def prepareOutput(line: String): String = {
    return line.replace("@outputs", "").trim
  }

  /**
    * This method remove all the blank spaces on the current line
    *
    * @param line line that we want eliminate the blank spaces
    * @return An string without any blank space
    */
  def removeBlankSpaces(line: String): String = {
    var temp: Array[String] = line.split("")
    var result = "";
    {
      for (line <- temp) {
        result += line.trim
      }
    }
    return result;
  }
}