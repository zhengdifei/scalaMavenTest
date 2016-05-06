/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.visenergy.scalaMavenTest.SparkStreaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import java.io.ByteArrayInputStream
import java.util.zip.ZipInputStream
import java.io.ByteArrayOutputStream
import scala.util.parsing.json.JSONObject
import scala.util.parsing.json.JSON

/**
 * kafka direct方式，测试
 */
object DirectKafkaWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
        |Usage: DirectKafkaWordCount <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
      //System.exit(1)
    }

    //val Array(brokers, topics) = args
    val Array(brokers, topics) = Array("localhost:9092","sensorData")
    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(6))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    def parseData(str:String):Array[String] = {
      //val tempStr=str.substring(0,str.length()-24)
      println(str)
      val out = new ByteArrayOutputStream()
      val zin = new ZipInputStream(new ByteArrayInputStream(new sun.misc.BASE64Decoder().decodeBuffer(str)))
      //zin.getNextEntry()
      var buffer:Array[Byte] = new Array[Byte](1024)
      var offset = zin.read(buffer)
      while(offset != -1){
        println(offset)
    	println(new sun.misc.BASE64Encoder().encodeBuffer(buffer))
    	offset = zin.read(buffer)
        //out.write(buffer,0,offset)
      }
      
      println(out.toString())
      
      return null
    }
    
//    def parseData2(str:String):Array[JSONObject] = {
//      val arrJson = new JSONArray(str)
//      val arr:Array[JSONObject] = new Array[JSONObject](arrJson.length())
//      for(i <- 0 to arrJson.length()-1){
//        arr(i) = arrJson.getJSONObject(i)
//      }
//      return arr
//     }
    
    def parseData3(str:String):Array[String] = {
      val afterR = str.replace("[", "").replace("]", "").replace("},{", "}&&{")
	  afterR.split("&&")
    }

    val ds1 = messages.map(x =>(x._2 ))
    val ds2 = ds1.flatMap(parseData3)
    val ds3 = ds2.map(JSON.parseFull(_)).map(_.get.asInstanceOf[scala.collection.immutable.Map[String, Any]])
    ds3.window(Seconds(12)).foreachRDD(rdd => {
      rdd.foreach(r => {
        println(r("SID"))
      })
    })
    //ds3.print()
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}