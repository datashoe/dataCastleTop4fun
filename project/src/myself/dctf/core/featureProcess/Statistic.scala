package myself.dctf.core.featureProcess

import myself.dctf.core.base.myAPP
import org.apache.spark.sql.DataFrame

object Statistic extends myAPP {
  override def run(): Unit = {
    val trainDataName = ""

    val trainDataFrame = outputrdd.get(trainDataName).asInstanceOf[DataFrame]


    /** 统计前7日付款和不付款的占比 */
    val pay_price = ""
    trainDataFrame.selectExpr()












  }
}
