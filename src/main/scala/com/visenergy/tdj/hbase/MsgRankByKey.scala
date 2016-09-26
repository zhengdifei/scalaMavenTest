package com.visenergy.tdj.hbase

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object MsgRankByKey {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("MsgRankByKey").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rddOp:RddOpUtil = new RddOpUtil()
    
    val handlerStartTime = System.currentTimeMillis()
    
    var msg = HBase2Spark.getHBaseRdd(sc, 0L, 4L)
    
    // 查询标签对应的排行榜
    val rankResult = rddOp.getMsgRank(msg, "SID")
	  val handlerEndTime = System.currentTimeMillis()
	  println("查询标签对应的排行榜耗费时间："+(handlerEndTime - handlerStartTime)/1000d+"s")
	  
	  rankResult.foreach(println)
	  
	  Thread.sleep(300000)
	  
  }
  
}