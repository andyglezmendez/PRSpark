package arnold.keel_adapter

import scala.io.Source

object MainParseCSV {
  def main(args: Array[String]): Unit = {
    val file = Source.getClass.getResource("/abalone19.dat")

    val adapter = new KeelParser(file.getPath)

    val r = adapter.convertFileToCSV("abalone19.csv")
  }
}
