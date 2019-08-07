package common.serialization

import common.miner.DatasetSchema
import org.junit.{Before, Test}

class ARFFSerializerTest {

  var ser: ARFFSerializer = null

  @Before
  def loadSerializer() = {
    ser = new ARFFSerializer()
  }

  //TODO Hacer este test
  @Test
  def load_db_name() = {
    val rows = ser.loadRowsFromARFF("D:\\Home\\School\\Data Mining\\BD\\zoo.arff")
    rows.foreach(println)
    val dm = new DatasetSchema(rows)
    dm.features().foreach(println)
    rows(0).schema.printTreeString()
    println(ser.dbName)
  }


}
