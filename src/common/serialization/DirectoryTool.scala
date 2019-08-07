package common.serialization

import java.nio.file.{Files, Paths}

import scala.collection.mutable.ArrayBuffer

object DirectoryTool {

  def loadFiles(path: String, pattern: String): Array[String] = {
    val files = new ArrayBuffer[String]()
    var directory = Files.newDirectoryStream(Paths.get(path), pattern)
    val iterator = directory.iterator()
    while (iterator.hasNext){
      files += iterator.next().toString
    }
    files.toArray
  }

  def fileName(path: String) = Paths.get(path).getFileName.toString

}
