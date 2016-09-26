package com.visenergy.tdj.hbase

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object QueryMsgBytesNum {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("QueryMsgBytesNum").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rddOp:RddOpUtil = new RddOpUtil()
    
    val handlerStartTime = System.currentTimeMillis()
    
    var msg = HBase2Spark.getHBaseRdd(sc, 0L, 4L)
    
    // 查看消息总字节数
	  println("查看消息总字节数： " + rddOp.getMsgBytesCount(sc, msg)) 
    val handlerEndTime = System.currentTimeMillis()
	  println("查看消息总字节数耗费时间："+(handlerEndTime - handlerStartTime)/1000d+"s")
    
	  Thread.sleep(300000)
    
  }
  
}