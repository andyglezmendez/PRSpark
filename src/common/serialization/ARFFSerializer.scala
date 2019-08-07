package common.serialization

import java.util.regex.Pattern

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

class ARFFSerializer {
  var dbName = ""
  var rows: Array[Row] = _

  private val DB_NAME = Pattern.compile("@relation '?([\\w_\\-\\d]+)'?", Pattern.CASE_INSENSITIVE)
  private val FEATURES =
    Pattern.compile("@attribute\\s+(?<name>('([^']+)')|([^\\s{]+))(?<description>[^%]+)(%(?<commentContent>.*))?",
      Pattern.CASE_INSENSITIVE)
  private val NOMINAL = Pattern.compile("""\s*\{(?<nominalValues>[^\}]+)\}""", Pattern.CASE_INSENSITIVE)
  private val INTEGER = Pattern.compile(
    """\s*integer(\s*\[\s*(?<minValue>\d+)\s*,\s*(?<maxValue>\d+)\])?""",
    Pattern.CASE_INSENSITIVE)
  private val REAL = Pattern.compile("""\s*(real|numeric)""", Pattern.CASE_INSENSITIVE)
  private val DATA = Pattern.compile("""@data""", Pattern.CASE_INSENSITIVE)

  /**
    * Load arff files to an array of Row
    *
    * @param file Path to arff file
    * @return Array of Row
    */
  def loadRowsFromARFF(file: String): Array[Row] = {
    val readed = Source.fromFile(file).getLines().toArray
    if (deserialize(readed)) return rows
    return null
  }

  /**
    * Load arff files to a DataFrame
    *
    * @param file Path to arff file
    * @return DataFrame
    */
  def loadDataFrameFromARFF(file: String): DataFrame = {
    val rows = loadRowsFromARFF(file)
    val spark = SparkSession.builder().getOrCreate()
    val rdd = spark.sparkContext.parallelize(rows)
    val df = spark.createDataFrame(rdd, rdd.first().schema)
    df.cache()
    df
  }

  /**
    * True if deserialize go well, false in other case
    *
    * @param readed
    * @return
    */
  private def deserialize(readed: Array[String]): Boolean = {
    for (i ← 0 to readed.length - 1) {
      val matcher = DB_NAME.matcher(readed(i))
      if (matcher.find()) {
        dbName = matcher.group(1)
        return featuresName(readed, i)
      }
    }
    return false
  }

  /**
    * Extract the features to the database schema
    *
    * @param readed
    * @param index
    * @return
    */
  private def featuresName(readed: Array[String], index: Int): Boolean = {
    val dataFields = new ArrayBuffer[StructField]()
    for (i ← index to readed.length - 1) {
      val matcher = FEATURES.matcher(readed(i))
      if (matcher.find()) {
        val name = matcher.group("name").replace(".", "-")
        val description = matcher.group("description").trim

        var dataType: DataType = null
        if (NOMINAL.matcher(description).matches())
          dataType = StringType
        else if (INTEGER.matcher(description).matches())
          dataType = IntegerType
        else if (REAL.matcher(description).matches())
          dataType = DoubleType

        if (dataType == null) throw new NullPointerException
        dataFields += new StructField(name, dataType, true)
      }
      else if (DATA.matcher(readed(i)).matches()) {
        val schema = new StructType(dataFields.toArray)
        return makeRows(readed, i, schema)
      }
    }
    return false
  }

  /**
    * Transform the @data to Rows
    *
    * @param readed
    * @param index
    * @param schema
    * @return
    */
  private def makeRows(readed: Array[String], index: Int, schema: StructType): Boolean = {
    val rowsValues = new ArrayBuffer[Row]()
    val dataFields = schema.fields
    for (i ← index to readed.length - 1) {
      val row = readed(i).trim().split(",")

      if (row.length == dataFields.length) {
        val values = new Array[Any](row.length)
        for (j ← 0 to row.length - 1) {
          if (row(j).equalsIgnoreCase("?"))
            values(j) = null
          else
            dataFields(j).dataType match {
              case StringType ⇒ values(j) = row(j);
              case IntegerType ⇒ values(j) = row(j).toInt;
              case DoubleType ⇒ values(j) = row(j).toDouble;
            }
        }
        rowsValues += new GenericRowWithSchema(values, schema)
      }
    }
    rows = rowsValues.toArray
    return rowsValues.length > 0
  }

}
