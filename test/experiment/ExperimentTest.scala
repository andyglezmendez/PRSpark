package experiment

import java.io.{FileWriter, PrintWriter}
import java.nio.file.Files

import org.junit.Test

class ExperimentTest {



  @Test
  def test()={
    val b = new StringBuilder()
    b.append("QWERTYU")
    b.append("ZXCVBNM")
    val a = new PrintWriter("test.txt")
    a.println(b)
    a.flush()
    a.close()


  }

}
