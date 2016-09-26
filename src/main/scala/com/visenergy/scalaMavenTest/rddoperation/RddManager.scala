package com.visenergy.scalaMavenTest.rddoperation

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.File

object RddManager {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("MsgProceedHandler").setMaster("local[2]")
    val sc = new SparkContext(conf)
    
    val msg = sc.textFile("/home/vis9/projects/testData/RddData/RddData.json")
    val rddOp:RddOperation = new RddOperation();
    
//    msg.persist()
//    
//    
    // 查询消息总数
    val handler1StartTime = System.currentTimeMillis()
    println("消息总数： " + rddOp.getMsgCount(msg))
    val handler1EndTime = System.currentTimeMillis()
	  println("查询消息总数耗费时间："+(handler1EndTime - handler1StartTime)/1000d+"s")
	  
	  // 查询每个传感器的消息数
	  val handler2StartTime = System.currentTimeMillis()
	  println("每个传感器的消息总数： " + rddOp.getMsgCountByKey(msg,"SID","C70B4291D8700001FDD62925130F1E17")) 
    val handler2EndTime = System.currentTimeMillis()
	  println("查询每个传感器的消息总数耗费时间："+(handler2EndTime - handler2StartTime)/1000d+"s")
    
	  // 查看消息总字节数
	  val handler3StartTime = System.currentTimeMillis()
	  println("查看消息总字节数： " + rddOp.getMsgBytesCount(sc, msg)) 
    val handler3EndTime = System.currentTimeMillis()
	  println("查看消息总字节数耗费时间："+(handler3EndTime - handler3StartTime)/1000d+"s")
	  
	  // 查询每个传感器的消息总字节数
	  val handler4StartTime = System.currentTimeMillis()
	  println("查询每个传感器的消息总字节数： " + rddOp.getMsgBytesCountByKey(sc,msg,"SID","C70B4291D8700001FDD62925130F1E17")) 
    val handler4EndTime = System.currentTimeMillis()
	  println("查询每个传感器的消息总字节数耗费时间："+(handler4EndTime - handler4StartTime)/1000d+"s")
	  
	  // 查询标签对应值的总和
    val handler5StartTime = System.currentTimeMillis()
    println("查询标签对应值的总和： " + rddOp.getSumByKey(msg,"COUNT"))
    val handler5EndTime = System.currentTimeMillis()
	  println("查询标签对应值的总和耗费时间："+(handler5EndTime - handler5StartTime)/1000d+"s")
	  
	  // 消息按照标签排序
    val handler6StartTime = System.currentTimeMillis()
    val sortedRdd = rddOp.sortByKey(msg,"COUNT")
    val handler6EndTime = System.currentTimeMillis()
	  println("消息按照标签排序耗费时间："+(handler6EndTime - handler6StartTime)/1000d+"s")
	  // 查看排序结果
	  val outputPath = "/home/vis9/projects/testData/RddData/result"
	  deleteDir(new File(outputPath))
	  sortedRdd.saveAsTextFile(outputPath)
	  
	  // 查询中位数，95%， 98%的记录
	  val handler7StartTime = System.currentTimeMillis()
//    println("查询排序后指定位置的记录： " + rddOp.getMedianNum(msg,"COUNT",99))
//    println("查询排序后指定位置的记录： " + rddOp.getSectionNum(msg,"COUNT",0.5))
    println("查询排序后指定位置的记录： " + rddOp.getSectionNumNew(msg,"COUNT",0.95)) 
	  val handler7EndTime = System.currentTimeMillis()
	  println("查询排序后指定位置的记录耗费时间："+(handler7EndTime - handler7StartTime)/1000d+"s")
	  
	  // 获取标签的and操作结果集  2-10
	  val handler8StartTime = System.currentTimeMillis()
    val andResult = rddOp.getAndOpMsg(msg, List(("SID", "C70B4291D8700001FDD62925130F1E17"), ("ram", "2GB"), ("rom", "16GB"))) 
	  val handler8EndTime = System.currentTimeMillis()
	  println("获取标签的and操作结果集耗费时间："+(handler8EndTime - handler8StartTime)/1000d+"s")
	  
	  // 查看结果
	  val andOutputPath = "/home/vis9/projects/testData/RddData/And"
	  deleteDir(new File(andOutputPath))
	  andResult.saveAsTextFile(andOutputPath)
	  
	  // 获取标签的or操作结果集
	  val handler9StartTime = System.currentTimeMillis()
    val orResult = rddOp.getOrOpMsg(msg, List(("SID", "C70B4291D8700001FDD62925130F1E17"), ("ram", "2GB"), ("rom", "16GB")))
	  val handler9EndTime = System.currentTimeMillis()
	  println("获取标签的or操作结果集耗费时间："+(handler9EndTime - handler9StartTime)/1000d+"s")
	  
	  // 查看结果
	  val orOutputPath = "/home/vis9/projects/testData/RddData/Or"
	  deleteDir(new File(orOutputPath))
	  orResult.saveAsTextFile(orOutputPath)
	  
	  // 查询标签对应的排行榜
	  val handler10StartTime = System.currentTimeMillis()
    val rankResult = rddOp.getMsgRank(msg, "ram")
	  val handler10EndTime = System.currentTimeMillis()
	  println("查询标签对应的排行榜耗费时间："+(handler10EndTime - handler10StartTime)/1000d+"s")
	  
	  
	  println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
	  rankResult.collect().foreach(println)
    
    // 查询颗粒度部分数据分布
	  val handler11StartTime = System.currentTimeMillis()
    val spanResult = rddOp.getTimeSpanDistributionNew(msg,"saleTime","COUNT",1356969600000L,1451577599000L,100L)
	  val handler11EndTime = System.currentTimeMillis()
	  println("查询颗粒度部分数据分布耗费时间："+(handler11EndTime - handler11StartTime)/1000d+"s")
	  
	  
	  println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
	  spanResult.collect().foreach(println)
    
    // 
	  
  }
  
  def deleteDir(dir : File) {
	  if(dir.isDirectory()){
	    val children = dir.listFiles()
	    for( x <- children){
	      deleteDir(x)
	    }
	    dir.delete()
	  }else{
	    dir.delete()
	  }
  }
}