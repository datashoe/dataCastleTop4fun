package myself.dctf.core.base

import java.util.HashMap

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

abstract class myAPP {
  lazy  val cfg  = new SparkConf().setAppName("zzjz-deepinsight-algorithm").setMaster("local[*]");
  lazy  val sc : SparkContext = new SparkContext(cfg);
  lazy  val sqlc : SQLContext = new SQLContext(sc);
  lazy val hqlc=sqlc
  lazy  val ssc : StreamingContext = new StreamingContext(cfg, Seconds(1))
  lazy val outputrdd: java.util.Map[java.lang.String,java.lang.Object]= new HashMap[java.lang.String,java.lang.Object]();

  def  run():Unit

  def main(args: Array[String]): Unit = {
    run()
  }

}
