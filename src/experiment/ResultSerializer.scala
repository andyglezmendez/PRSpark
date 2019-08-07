package experiment

import java.io.PrintWriter
import java.nio.file.{Files, Paths}
import java.util.regex.Pattern

import common.serialization.DirectoryTool

import scala.io

trait ResultSerializer {

  def saveData(path: String, data: DataResult): Unit = {
    val str = new StringBuilder

    if (!Files.exists(Paths.get(path)))
      str.append(data.header())
    else {
      val old = io.Source.fromFile(path).mkString
      str.appendAll(old)
    }

    str.append(data.toString)
    val writer = new PrintWriter(path)
    writer.write(str.mkString)
    writer.flush()
    writer.close()
  }

  def deletePreviousResults(path: String, pattern: String): Unit ={
//    val files = DirectoryTool.loadFiles(path, pattern)
//    files.foreach(file ⇒ Files.delete(Paths.get(file)))
  }

  def dbName(path: String): String = {
    var regexp = Pattern.compile( """(?<=\\)(?<db>[^\\]*)(?=\.)""").matcher(path)
    if (regexp.find())
      return regexp.group("db")
    return "DATABASE"
  }
}
