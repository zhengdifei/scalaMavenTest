package com.visenergy.tdj.hbase

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object QueryMsgCount {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("QueryMsgCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rddOp:RddOpUtil = new RddOpUtil()
    
    val handlerStartTime = System.currentTimeMillis()
    
    var msg = HBase2Spark.getHBaseRdd(sc, 0L, 4L)
    
    // 查询消息总数
    println("消息总数： " + rddOp.getMsgCount(msg))
    val handlerEndTime = System.currentTimeMillis()
	  println("查询消息总数耗费时间："+(handlerEndTime - handlerStartTime)/1000d+"s")
	  
	  Thread.sleep(300000)
	  
  }
}