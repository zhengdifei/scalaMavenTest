package com.visenergy.scalaMavenTest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.util.parsing.json.JSON
import java.io.File
import org.json.JSONObject
import org.apache.spark.rdd.RDD

object SensorCount2 {
	def main(args:Array[String]){
	  println("服务参数:" + args)
	  for(x <- args){
	    println(x)
	  }
	  
	  //var sparkServer = "local"
	  var sparkServer = "local[2]"
	  if(args.length > 0 && args(0) != None){
	    sparkServer = args(0)
	  }
	  var inputPath = "test/test*.json"
	  //var inputPath = "hdfs://localhost:9000/input/data2/*"
	  
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
	  val rdd3 = rdd2.map(jsonParse)
	  rdd3.persist()
	  val changeTime = System.currentTimeMillis()
	  println("RDD数据处理完："+(changeTime - startTime)/1000+"s")
	  //数量
	  val rdd4 = rdd3.map(x => (x.get("SID"),1))
	  val rdd5 = rdd4.reduceByKey(_ + _)
	  //rdd5.saveAsTextFile(outputPath)
	  val handler1Time = System.currentTimeMillis()
	  println("数量："+(handler1Time - changeTime)/1000+"s")
	  //rdd5.foreach(println)
	  //求和
	  val rdd6 = rdd3.map(x => (x.get("SID"),x.get("UA").toString.toDouble))
	  val rdd7 = rdd6.reduceByKey(_ + _)
	  //rdd7.saveAsTextFile(outputPath2)
	  val handler2Time = System.currentTimeMillis()
	  println("求和："+(handler2Time - startTime)/1000+"s")
	  //rdd7.foreach(println)
	  /**
	   * 迭代求中位数
	   * rdd:数据集合
	   * partNum : rdd分片数
	   * midPartNum : 中间分片index
	   * midCountNum : 中位数下标
	   */
	  def getMidNum(rdd:RDD[Double],partNum:Int,midPartNum:Int,midCountNum:Int):(RDD[Double],Int) = {
		  //println(partNum +":"+ midPartNum + ":"+ midCountNum +"@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
		  //中位数在第一个分区中,或者中位数落在最后一个分区中，数据分配不均
		  if(midPartNum == 0){
		    return (rdd,midCountNum)
		  }
		  //求中位数
		  val rdd1 = rdd.sortBy(x=>x, true, partNum)
		  var acc = sc.accumulator(0)
		  //迭代缩小范围
		  val rdd2 = rdd1.mapPartitionsWithIndex{
		      (partIndex,iter) => {
		    	    //如何打印iter.size，则acc1，rdd9.count一直为0
				    //println(partIndex + ":" + iter.size)
				    var result = List[Double]()
				    if(partIndex == midPartNum){
				      while(iter.hasNext){
				    	  result ::=(iter.next)
				      }
				    }
				    if(partIndex < midPartNum){
				      acc += iter.size
				    }
				    result.iterator
		      }
		  }
		  //子结合数目
		  val rdd2Count = rdd2.count
		  //返回结果
		  var result = (rdd2,midCountNum-acc.value-1)
		  //中位数落在midPartNum小集合中
		  if(acc.value > midCountNum){
		      //println(acc.value +":"+ midCountNum +"&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
		      result = getMidNum(rdd,partNum,midPartNum-1,midCountNum)
		  //中位数落在midPartNum大的机会中
		  }else if((acc.value + rdd2Count) < midCountNum){
		      //println(acc.value +":"+ midCountNum +"**********************************")
		      result = getMidNum(rdd,partNum,midPartNum+1,midCountNum)
		  //中位数正好落在midCountNum分区中
		  }else if(acc.value + rdd2Count >= midCountNum){
		     //println(acc.value +":"+ midCountNum +"%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
		     //如果发现子分区数量>20000,继续进行迭代，缩小范围
		     if(rdd2Count > 20000 ){
		    	 result = getMidNum(rdd2,partNum,midPartNum,midCountNum-acc.value-1)
		     }
		  //其他异常情况
		  }else{
		     //println(acc.value +":"+ midCountNum +"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
		  }
		  
		  result
	  }
	  
	  val rdd8 = rdd3.map(x => x.get("UA").toString().toDouble)
	  val countNum = rdd8.count
	  val partNum = 11
	  val midPartNum = partNum/2 - 1 + partNum%2
	  val midCountNum = countNum/2 + countNum%2
	  val rdd9 = getMidNum(rdd8,partNum,midPartNum,midCountNum.toInt)
	  println("mid:" + rdd9._2 )
	  println("count:" + rdd9._1.count )
	  val rdd9Arr = rdd9._1.sortBy(x=>x,true).collect
	  println("rdd9mid:" + rdd9Arr(rdd9._2))
	  val handler3Time = System.currentTimeMillis()
	  println("中位数："+(handler3Time - handler2Time)/1000+"s")
	  /**
	   * 求中位数，缩小10倍求中位数
	   */
//	  val partNum = 11
//	  val midPartNum = partNum/2 - 1 + partNum%2
//	  val rdd8 = rdd3.map(x => x.get("UA").toString().toDouble).sortBy(x=>x, true, partNum)
//	  val countNum = rdd8.count
//	  val midCountNum = countNum/2 + countNum%2
//	  var acc1 = sc.accumulator(0)
//	  val rdd9 = rdd8.mapPartitionsWithIndex{
//	      (partIndex,iter) => {
//	    	    //如何打印iter.size，则acc1，rdd9.count一直为0
//			    //println(partIndex + ":" + iter.size)
//			    var result = List[Double]()
//			    if(partIndex == midPartNum){
//			      while(iter.hasNext){
//			    	  result ::=(iter.next)
//			      }
//			    }
//			    if(partIndex < midPartNum){
//			      acc1 += iter.size
//			    }
//			    result.iterator
//	      }
//	  }  
//	  val rddCount = rdd9.count
//	  val accnum = acc1.value
//	  println(s"$accnum : $rddCount : $midCountNum")
//	  val rdd9Arr = rdd9.sortBy(x=>x,true).collect
//	  println("rdd9mid:" + rdd9Arr((midCountNum - accnum -1).toInt))
//	  val handler3Time = System.currentTimeMillis()
//	  println("求和："+(handler3Time - handler2Time)/1000+"s")
	 /**
	 * 将数据放在driver中，求中位数
	 */
//	  val rdd8Arr = rdd8.collect
//	  
//	  println("rdd8mid:" + rdd8Arr((midCountNum-1).toInt))
//	  val handler4Time = System.currentTimeMillis()
//	  println("求和："+(handler4Time - handler2Time)/1000+"s")
	}
}