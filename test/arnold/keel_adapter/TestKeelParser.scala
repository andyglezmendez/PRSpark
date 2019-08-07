package arnold.keel_adapter

import org.junit.{Test}
import scala.io.Source

class TestKeelParser {

  @Test
  def testKeelParserMethod: Unit = {

    val file = Source.getClass.getResource("/glass1.dat")
    val adapter = new KeelParser(file.getPath)
    adapter.parseDatabase()

    val a1 = adapter.getRelationName()
    val a2 = adapter.getAttributes()
    val a3 = adapter.getData()

    assert(a1 != null)
    assert(a2 != null)
    assert(a3 != null)
  }

  @Test
  def testKeelParserConvertingToCSV: Unit = {

    val file = Source.getClass.getResource("/glass1.dat")

    val adapter = new KeelParser(file.getPath)
    val r = adapter.convertFileToCSV("glass1.csv")
    assert(r)
  }

  @Test
  def testKeelParserRowGeneration: Unit = {

    val databaseName = "/wisconsin.dat"
    val file = Source.getClass.getResource(databaseName)
    val adapter = new KeelParser(file.getPath)
    adapter.parseDatabase()

    val rowList = adapter.getRowList()

    var countPositive = 0
    var countNegative = 0
    var countOtherClass = 0

    var positiveRepresentation = ""
    var negativeRepresentation = ""

    var otherClass = ""

    for (row <- rowList) {
      if (databaseName.equals("/adult.dat")) {
        positiveRepresentation = "<=50K"
        negativeRepresentation = ">50K"

        if (row.getString(row.schema.fields.length - 1).equals(positiveRepresentation))
          countPositive += 1
        else countNegative += 1
      }
      else if (databaseName.equals("/wine.dat")) {
        positiveRepresentation = "1"
        negativeRepresentation = "2"
        otherClass = "3"

        if (row.getString(row.schema.fields.length - 1).equals(positiveRepresentation))
          countPositive += 1
        else if (row.getString(row.schema.fields.length - 1).equals(negativeRepresentation))
          countNegative += 1
        else countOtherClass += 1
      }
      else {

        positiveRepresentation = "positive"
        negativeRepresentation = "negative"
        if (row.getString(row.schema.fields.length - 1).equals(positiveRepresentation))
          countPositive += 1
        else countNegative += 1
      }

    }

    println("Instancias positivas " + countPositive + " Representacion Positiva :" + positiveRepresentation)
    println("Instancias negativas " + countNegative + " Representacion Negativa :" + negativeRepresentation)

    if (!otherClass.equals(""))
      println("CLase extra1 clase " + countOtherClass + " Representacion Clase :" + otherClass)
  }
}
