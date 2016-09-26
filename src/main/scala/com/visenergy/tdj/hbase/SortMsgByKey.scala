package com.visenergy.tdj.hbase

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SortMsgByKey {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("SortMsgByKey").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rddOp:RddOpUtil = new RddOpUtil()
    
    val handlerStartTime = System.currentTimeMillis()
    var msg = HBase2Spark.getHBaseRdd(sc, 0L, 99L)
    
    // 消息按照标签排序
    val sortedRdd = rddOp.sortByKey(msg,"COUNT")
    val handlerEndTime = System.currentTimeMillis()
	  println("消息按照标签排序耗费时间："+(handlerEndTime - handlerStartTime)/1000d+"s")
	  
	  sortedRdd.collect().foreach(println)
	  
	  Thread.sleep(300000)
	  
  }
}