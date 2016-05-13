package com.visenergy.tdj.hbase

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.immutable.List

object QueryDataDistributionByKey {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("QueryDataDistributionByKey").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rddOp:RddOpUtil = new RddOpUtil()
    
    val handlerStartTime = System.currentTimeMillis()
    
    var msg = HBase2Spark.getHBaseRdd(sc, 0L, 4L)
    
    // 查询颗粒度部分数据分布
    val spanResult = rddOp.getTimeSpanDistributionNew(msg,"saleTime","COUNT",1356969600000L,1451577599000L,30L)
	  val handlerEndTime = System.currentTimeMillis()
	  println("查询颗粒度部分数据分布耗费时间："+(handlerEndTime - handlerStartTime)/1000d+"s")
    
	  spanResult.foreach(println)
	  
    Thread.sleep(300000)
	  
  }
}