package com.visenergy.scalaMavenTest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.util.parsing.json.JSON
import java.io.File

object SensorCount {
	def main(args:Array[String]){
	  var sparkServer = "local"
	  if(args.length > 0 && args(0) != None){
	    sparkServer = args(0)
	  }
	  
	  var inputPath = "test/sensorData1.json"
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
	  
	  val conf = new SparkConf().setAppName("sensorTest").setMaster(sparkServer)
	  val sc = new SparkContext(conf)
	  
	  val rdd1 = sc.textFile(inputPath)
	  
	  def strReplace(s : String):Array[String] ={
		  val afterR = s.replace("][",",").replace("[", "").replace("]", "").replace("},{", "}&&{")
		  afterR.split("&&")
	  }
	  
	  def jsonParse(s:String):Map[String, Any] ={
		  val b = JSON.parseFull(s)
		  b match {
		    case Some(map: Map[String, Any]) => return map
		  }
	  }
	  
	  val rdd2 = rdd1.flatMap(strReplace)
	  val rdd3 = rdd2.map(jsonParse)
	  rdd3.persist()
	  //数量
	  val rdd4 = rdd3.map(x => (x.get("SID").get,1))
	  val rdd5 = rdd4.reduceByKey(_ + _)
	  rdd5.saveAsTextFile(outputPath)
	  //rdd5.foreach(println)
	  //求和
	  val rdd6 = rdd3.map(x => (x.get("SID"),x.get("UA").get.toString.toDouble))
	  val rdd7 = rdd6.reduceByKey(_ + _)
	  rdd7.saveAsTextFile(outputPath2)
	  //rdd7.foreach(println)
	}
}