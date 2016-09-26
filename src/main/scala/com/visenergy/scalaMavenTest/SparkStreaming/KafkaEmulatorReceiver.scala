package com.visenergy.scalaMavenTest.SparkStreaming

import java.util.HashMap
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import scala.util.parsing.json.JSON
import org.json.JSONObject
import java.io.Serializable
/**
 * Receiver 方式读取streaming
 */
object KafkaEmulatorReceiver {
	    def main(args: Array[String]) {
		
		   // StreamingExamples.setStreamingLogLevels()
		
		   // val Array(zkQuorum, group, topics, numThreads) = args
		    val sparkConf = new SparkConf().setAppName("KafkaEmulatorReceiver").setMaster("local[2]")
		    val ssc = new StreamingContext(sparkConf, Seconds(6))
		    ssc.checkpoint("checkpoint")
		
//		    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
		    val lines = KafkaUtils.createStream(ssc, "localhost:2181", "group",  Map("sensorData" -> 1), StorageLevel.MEMORY_AND_DISK_SER_2).map(_._2)
		  //  lines.print()
		    def strMath(s:String):Array[String]={
		    	//截取MD5码
			    val tempStr=s.substring(0,s.length()-24).replace("[", "").replace("]", "").replace("},{", "}&&{")
			    tempStr.split("&&")
	        }
            def jsonParse(s:String):JSONObject ={
				 new JSONObject(s)
			}
            val rdd2=lines.flatMap(strMath);
		  //  println("数据总量为"+rdd2.count)
			val rdd3=rdd2.map(jsonParse)
			rdd3.persist()
			val rdd4 = rdd3.map(x => (x.get("UA"),1))
			val rdd7=rdd4.reduceByKey(_ + _)
			val rdd8=rdd3.map(x => x.get("UA").toString.toDouble)
			rdd8.reduce(_+_).print()
			println(" UA的总和"+ rdd8.reduce(_+_))
		    ssc.start()
		    ssc.awaitTermination()
	  }
}