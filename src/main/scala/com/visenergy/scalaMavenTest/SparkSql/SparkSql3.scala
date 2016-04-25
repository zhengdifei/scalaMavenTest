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

object SparkSql3 {
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
	  deleteDir(new File("test/sensorData.parquet"))
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
	   //println(s)
		 new JSONObject(s)
	  }
	  
	  val rdd2 = rdd1.flatMap(strReplace)
	  
	  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
	  import sqlContext.implicits._
	  val rdd4 = sqlContext.jsonRDD(rdd2)
	  
	  //新建一个parquet文件
	  rdd4.registerTempTable("rdd4")
	  rdd4.select("SID","SNAME", "UA","UB","UC").write.format("parquet").save("test/sensorData.parquet")
	  //读取parquet文件
	  val parquetFile = sqlContext.read.parquet("test/sensorData.parquet")
	  parquetFile.registerTempTable("parquetFile")
	  //val rdd5 = sqlContext.sql("SELECT SID,UA,UB,UC,SNAME FROM parquetFile WHERE UA >= 60.25 AND UA <= 60.95")
	  val rdd5 = sqlContext.sql("SELECT SID,SUM(UA),COUNT(SNAME) FROM parquetFile group by SID")
	  rdd5.saveAsParquetFile(outputPath2)
	  val handler2Time = System.currentTimeMillis()
	  println("求和："+(handler2Time - startTime)/1000+"s")
  }
}