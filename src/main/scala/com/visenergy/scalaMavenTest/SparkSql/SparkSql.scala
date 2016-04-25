package com.visenergy.scalaMavenTest.SparkSql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.util.parsing.json.JSON
import java.io.File
import org.json.JSONObject


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext


object SparkSql {
  def main(args:Array[String]){
	  var sparkServer = "local"
	  if(args.length > 0 && args(0) != None){
	    sparkServer = args(0)
	  }
	  
	  var inputPath = "test/test*.json"
	  //var inputPath = "hdfs://localhost:9000/input/test/"
	  
	  if(args.length > 1 && args(1) != None){
	    inputPath = args(1)
	  }
	  
	  var outputPath = "test/result5"
	  var outputPath2 = "test/result6"
	   def deleteDir(dir : File){
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
	  
	  deleteDir(new File(outputPath))
	  deleteDir(new File(outputPath2))
	  
	  if(args.length > 2 && args(2) != None){
	    outputPath = args(2)
	  }
	  
	  val startTime = System.currentTimeMillis()
	  val conf = new SparkConf().setAppName("sensorTest").setMaster(sparkServer)
	  val sc = new SparkContext(conf)
	  
	  val rdd1 = sc.textFile(inputPath)
	  
	  def strReplace(s : String):Array[String] ={
		  val afterR = s.replace("][",",").replace("[", "").replace("]", "").replace("},{", "}&&{")
		  afterR.split("&&")
	  }
	  
	  def jsonParse(s:String):JSONObject ={
		 new JSONObject(s)
	  }
	  
	  val rdd2 = rdd1.flatMap(strReplace)
	  
	  
	  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
	  import sqlContext.implicits._
	  val rdd4 = sqlContext.jsonRDD(rdd2)
	  rdd4.persist()
	  //计算的数据保存为txt文件
	  rdd4.registerTempTable("rdd4")
	  //数量
	  val totalCOUNT = sqlContext.sql("select SID,sum(COUNT) as total from rdd4 group by SID")
	  //求和
	  val totalUA = sqlContext.sql("select SID,sum(UA) as total from rdd4 group by SID")
	  val rdd5 = totalCOUNT.map(x => (x.getAs("SID"),x.getAs("total")))
	  val rdd6 = totalUA.map(x => (x.getAs("SID"),x.getAs("total")))
	  rdd5.saveAsTextFile(outputPath)
	  rdd6.saveAsTextFile(outputPath2)
	  val handler2Time = System.currentTimeMillis()
	  println("求和："+(handler2Time - startTime)/1000+"s")	  
	  
  }
}