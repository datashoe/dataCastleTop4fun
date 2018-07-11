package myself.dctf.core.featureProcess

import myself.dctf.core.base.myAPP

object ReadFile extends myAPP {
  override def run(): Unit = {
    /** 读取csv数据，第一行为表头 */
    val train_path = "/data/dctf/tap_fun_test.csv"
    val test_path = "/data/dctf/tap_fun_train.csv"

    val train_data = sqlc.read.format("com.databricks.spark.csv")
      .option("header", "true").option("inferSchema", true.toString).load(train_path)

    val test_data = sqlc.read.format("com.databricks.spark.csv")
      .option("header", "true").option("inferSchema", true.toString).load(test_path)

    val rddTableName = "<#rddtablename#>"

    train_data.registerTempTable(rddTableName + "_trainData")
    sqlc.cacheTable(rddTableName + "_trainData")
    outputrdd.put(rddTableName + "_trainData", train_data)

    test_data.registerTempTable(rddTableName + "_testData")
    sqlc.cacheTable(rddTableName + "_testData")
    outputrdd.put(rddTableName + "_testData", test_data)






  }
}
