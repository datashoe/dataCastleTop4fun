package myself.dctf.core.featureProcess

import myself.dctf.core.base.myAPP
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object Statistic extends myAPP {
  override def run(): Unit = {
    val trainDataName = ""
    println("good")

    val trainDataFrame = outputrdd.get(trainDataName).asInstanceOf[DataFrame]

    val colNames = trainDataFrame.schema.fieldNames

    /** 将数据取出来 */
    val idCol = "user_id"
    val regTimeCol = "register_time"
    val predictCol = "prediction_pay_price"

    /** 将值分箱 */



    val features = colNames.filterNot(tup => Array(idCol, regTimeCol, predictCol) contains tup)
    val featureNums = features.length

    val rawData = trainDataFrame.rdd.map(row => {
      val id = row.getAs[Int](idCol)
      val predict = row.getAs[Double](predictCol)

      val arr = colNames.zipWithIndex.filterNot(tup => Array(idCol, regTimeCol, predictCol) contains tup._1).map {
        case (_, index) => row.get(index) match {
          case i: Int => i.toDouble
          case d: Double => d
          case f: Float => f.toDouble
        }
      }

      (id, LabeledPoint(predict, new DenseVector(arr)))
    })


    /** 使用k折方法 */
    var seed = 123L
    var i = 0
    var sum = new Array[Double](featureNums)
    while (i < 10) {
      val Array(trainDT, _) = rawData.randomSplit(Array(0.7, 0.3), seed)

      val chiValues = Statistics.chiSqTest(trainDT.values).map(_.statistic)
      sum = sum.zip(chiValues).map { case (d1, d2) => d1 + d2 }
      seed = seed.hashCode() % 11324
      i += 1
    }

    val chiValues = features.zip(sum).sortBy(-_._2) // 降序排列

    val fff = Array(idCol, regTimeCol, predictCol).map(col) ++ chiValues.map { case (name, _) => col(name) }

    val newDataFrame = trainDataFrame.select(fff: _*)

    newDataFrame.registerTempTable("<#zzjzRddName#>")
    newDataFrame.sqlContext.cacheTable("<#zzjzRddName#>")
    outputrdd.put("<#zzjzRddName#>", newDataFrame)

    outputrdd.put("<#zzjzRddName#>_chiValues", chiValues)


  }
}
