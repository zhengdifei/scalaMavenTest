package com.visenergy.tdj.hbase

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.immutable.List

object OrOpByKey {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("OrOpByKey").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rddOp:RddOpUtil = new RddOpUtil()
    
    val handlerStartTime = System.currentTimeMillis()
    var msg = HBase2Spark.getHBaseRdd(sc, 0L, 4L)
    
    // 获取标签的or操作结果集
    val orResult = rddOp.getOrOpMsg(msg, List(("SID", "C70B4291D8700001FDD62925130F1E17"), ("SNAME", "VIS-SENSOR-0")))
	  val handlerEndTime = System.currentTimeMillis()
	  println("获取标签的or操作结果集耗费时间："+(handlerEndTime - handlerStartTime)/1000d+"s")
  
	  orResult.foreach(println)
	  
    Thread.sleep(300000)
    
  }
}