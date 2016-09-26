package com.visenergy.tdj.hbase

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object QuerySumByKey {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("QuerySumByKey").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rddOp:RddOpUtil = new RddOpUtil()
    
    val handlerStartTime = System.currentTimeMillis()
    var msg = HBase2Spark.getHBaseRdd(sc, 0L, 99L)
    
    // 查询标签对应值的总和
    println("查询标签对应值的总和： " + rddOp.getSumByKey(msg,"COUNT"))
    val handlerEndTime = System.currentTimeMillis()
	  println("查询标签对应值的总和耗费时间："+(handlerEndTime - handlerStartTime)/1000d+"s")
	  
	  Thread.sleep(300000)
	  
  }
}