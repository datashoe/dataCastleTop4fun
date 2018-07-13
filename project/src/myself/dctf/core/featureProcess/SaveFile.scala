package myself.dctf.core.featureProcess

import myself.dctf.core.base.myAPP
import org.apache.spark.sql.DataFrame

object SaveFile extends myAPP {
  override def run(): Unit = {
    val trainDataName = ""

    val trainDataFrame = outputrdd.get(trainDataName).asInstanceOf[DataFrame]

    val rr = trainDataFrame.randomSplit(Array(0.15, 0.15, 0.15, 0.15, 1 - 0.18 * 4), 123L)

    val path = "/data/dctf/sample"
    var i = 0

    rr.foreach {
      data =>
        data.rdd.map(row => row.toSeq.map(_.toString).mkString(",")).repartition(1).saveAsTextFile(path + "_" + i + ".txt")
        i += 1
    }



  }
}
