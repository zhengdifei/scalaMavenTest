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
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import scala.util.parsing.json.JSON
/**
 * 显示kafka的读取的offset
 */
object DirectKafkaWordCount5 {
  
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
    val Array(brokers, topics) = Array("192.168.100.11:9092","sensorData")
    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount3").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(6))
    val sqc = new SQLContext(sc)

    ssc.checkpoint("test/point")
    import sqc._
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    //auto.offset.reset=largest
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,"auto.offset.reset"->"largest")
    val initOffsetRange = Array(OffsetRange("sensorData",0,1281,1281))
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    
    def parseData(str:String):Array[String] = {
      val afterR = str.replace("[", "").replace("]", "").replace("},{", "}&&{")
	  afterR.split("&&")
    }
    
    val ds1 = messages.map(x =>(x._2 ))
    val ds2 = ds1.flatMap(parseData)
    val ds3 = ds2.map(JSON.parseFull(_)).map(_.get.asInstanceOf[scala.collection.immutable.Map[String, Any]])
    val ds4 = ds3.map(x=>x("SID").toString->1)
    
    messages.foreachRDD(rdd =>{
      var offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for(o <- offsetRanges){
        println(s"${o.topic }  ${o.partition }  ${o.fromOffset }  ${o.untilOffset }")
      }
      
      rdd.foreach(println)
    })
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}