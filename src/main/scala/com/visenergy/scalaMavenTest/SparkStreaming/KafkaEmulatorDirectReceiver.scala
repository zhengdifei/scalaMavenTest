package com.visenergy.scalaMavenTest.SparkStreaming

import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
/**
 * DirectReceiver 方式读取streaming
 */
object KafkaEmulatorDirectReceiver {
	     def main(args : Array[String]) {
	       // = Array("localhost:2181","sensorData")
		   // StreamingExamples.setStreamingLogLevels()
		   // val Array(brokers, topics) = args
		
		    // Create context with 2 second batch interval
		    val sparkConf = new SparkConf().setAppName("KafkaEmulatorDirectReceiver").setMaster("local[2]")
		    val ssc = new StreamingContext(sparkConf, Seconds(2))
		    ssc.checkpoint("checkpoint")
		    // Create direct kafka stream with brokers and topics
		   // val topicsSet = topics.split(",").toSet//此处topic写死
		    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
		    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
		      ssc, kafkaParams,  Set("sensorData"))
		    def strMath(s:String):Array[String]={
		    	//截取MD5码
			    val tempStr=s.substring(0,s.length()-24).replace("[", "").replace("]", "").replace("},{", "}&&{")
			    tempStr.split("&&")
	        }
		    val lines = messages.map(_._2)
		    lines.print()
		
		    // Start the computation
		    ssc.start()
		    ssc.awaitTermination()
  }
}