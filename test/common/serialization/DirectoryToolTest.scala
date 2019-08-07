package common.serialization
import org.junit.{Before, Test, Assert}

class DirectoryToolTest {

  @Test
  def load_files_test(): Unit ={
    val files = DirectoryTool.loadFiles("D:\\Home\\School\\Data Mining\\BD", "*.arff")
    Assert.assertEquals(files.length, 71)
  }


}
