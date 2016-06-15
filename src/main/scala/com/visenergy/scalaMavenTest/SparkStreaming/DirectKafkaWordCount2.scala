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
import org.apache.hadoop.io.NullWritable
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
/**
 * spark streaming与spark sql结合
 */
object DirectKafkaWordCount2 {
  
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
    val Array(brokers, topics) = Array("node13:9092,node14:9092","sensorData")
    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount2").setMaster("local[2]")
    //只是忽然错误，并不是允许创建两个SparkContext对象
    sparkConf.set("spark.driver.allowMultipleContexts","true")
    val sc = new SparkContext(sparkConf)
    
    val sc1 = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc1, Seconds(6))
    val sqc = new SQLContext(sc1)
    
    
    
    import sqc._
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    
    def parseData(str:String):Array[String] = {
      val afterR = str.replace("[", "").replace("]", "").replace("},{", "}&&{")
	  afterR.split("&&")
    }
    val mapper = new ObjectMapper()
     
    val ds1 = messages.map(x =>(x._2 ))
    val ds2 = ds1.flatMap(parseData)
    ds2.print()
//    ds2.foreachRDD(rdd => {
        //RDD,DataFrame的存储都是不带时间标识的，会覆盖或者报错
    	//保存方式
    	//rdd.map(x=>(NullWritable.get(),x)).saveAsSequenceFile("test/kryo/zdf")
    	//json方式保存
        //rdd.saveAsTextFile("test/json/zdf1")
    	//rdd.map(mapper.writeValueAsString(_)).saveAsTextFile("test/json/zdf2")
//    	if(!rdd.isEmpty){
//    	  val t = sqc.jsonRDD(rdd)
//    	  //t.printSchema()
//    	  t.registerTempTable("eData")
//    	  val sqlReport = sqc.sql("select SNAME,count(SNAME) as num,AVG(UA) avg_ua,sum(jg) as sum_jg from eData group by SNAME order by sum_jg")
//    	  sqlReport.save("test/table/zdf")
//    	}
//    })
    //println(ds2.count())
    //保存本地文件
    //ds2.saveAsTextFiles("test/txt/zdf","json")
    //ds2.saveAsObjectFiles("test/obj/zdf")

    //保存到hdfs
    //ds2.saveAsTextFiles("hdfs://localhost:9000/test/zdf")
    //ds3.print()
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}