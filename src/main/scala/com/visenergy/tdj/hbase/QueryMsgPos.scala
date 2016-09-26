package com.visenergy.tdj.hbase

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object QueryMsgPos {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("QueryMsgPos").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rddOp:RddOpUtil = new RddOpUtil()
    
    val handlerStartTime = System.currentTimeMillis()
    var msg = HBase2Spark.getHBaseRdd(sc, 0L, 99L)
    
    msg.foreach(println)
    // 查询中位数，95%， 98%的记录
    println("查询排序后指定位置的记录： " + rddOp.getSectionNumNew(msg,"COUNT",0.5)) 
	  val handlerEndTime = System.currentTimeMillis()
	  println("查询排序后指定位置的记录耗费时间："+(handlerEndTime - handlerStartTime)/1000d+"s")
	  
	  Thread.sleep(300000)
	  
  }
  
}