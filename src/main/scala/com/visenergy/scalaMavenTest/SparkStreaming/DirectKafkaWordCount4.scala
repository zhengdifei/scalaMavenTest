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
import org.apache.spark.HashPartitioner
/**
 * 从checkPoint恢复状态，失败
 */
object DirectKafkaWordCount4 {
  
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
    def createStreamingContext():StreamingContext = {
      val conf = new SparkConf().setAppName("SparkStreamingTest4").setMaster("local[2]");

      val ssc = new StreamingContext(conf,Seconds(6));
      
       ssc.checkpoint("hdfs://localhost:9000/test/point1")
       
       return ssc
    }
    //?
    val ssc = StreamingContext.getOrCreate("hdfs://localhost:9000/test/point1", createStreamingContext _)
    
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
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
    
    def addFunc1(values : Seq[Int],state:Option[Int]) = {
      println(values)
      println(state)
      Some(state.getOrElse(0)+values.size)
    }
    
    val updateStateValues = (iterator : Iterator[(String,Seq[Int],Option[Int])]) => {
      iterator.flatMap(t => {
        println(t)
        val newValue:Int = t._2.size 
        val stateValue:Int = t._3.getOrElse(0)
        Some(newValue + stateValue)
      }.map(x=>(t._1 ,x)))
    }
    
    //val initialRDD = ssc.sparkContext.parallelize(List(("test1",0)))
    val totalWordCounts = ds4.updateStateByKey(addFunc1)

    //val totalWordCounts = ds4.updateStateByKey(updateStateValues,new HashPartitioner(ssc.sparkContext.defaultParallelism),true,initialRDD)
    
    totalWordCounts.checkpoint(Duration(6*1000))

    totalWordCounts.print
    //ds3.print()
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}