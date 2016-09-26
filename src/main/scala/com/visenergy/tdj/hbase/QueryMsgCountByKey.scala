package com.visenergy.tdj.hbase

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object QueryMsgCountByKey {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("QueryMsgCountByKey").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rddOp:RddOpUtil = new RddOpUtil()
    val handlerStartTime = System.currentTimeMillis()
    
    var msg = HBase2Spark.getHBaseRdd(sc, 0L, 4L)
    
    // 查询每个传感器的消息数
	  println("每个传感器的消息总数： " + rddOp.getMsgCountByKey(msg,"SID","C70B4291D8700001FDD62925130F1E17")) 
    val handlerEndTime = System.currentTimeMillis()
	  println("查询每个传感器的消息总数耗费时间："+(handlerEndTime - handlerStartTime)/1000d+"s")
	  
	  Thread.sleep(300000)
	  
  }
}