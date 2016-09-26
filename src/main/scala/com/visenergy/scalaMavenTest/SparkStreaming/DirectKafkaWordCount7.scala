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
import redis.clients.jedis.JedisPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
/**
 * spark streaming与spark sql结合+redis
 */
object DirectKafkaWordCount7 {
  
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
    val Array(brokers, topics) = Array("localhost:9092","twoPartitionTopic")
    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount2").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(6))
    val sqc = new SQLContext(sc)

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
    
    val ds1 = messages.map(x =>(x._2 ))
    val ds2 = ds1.flatMap(parseData)
    ds2.foreachRDD(rdd => {
    	rdd.foreachPartition(partitionsRecords => {
    	  partitionsRecords.foreach(pair => {
    	    object InternalRedisClient extends Serializable {
    	      @transient private var pool:JedisPool = null
    	      
    	      def makePool(redisHost:String,redisPort:Int,redisTimeout:Int,
    	          maxTotal:Int,maxIdle:Int,minIdle:Int):Unit = {
    	        makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle, true, false, 10000)
    	      }
    	      
    	      def makePool(redisHost:String,redisPort:Int,redisTimeout:Int,
    	          maxTotal:Int,maxIdle:Int,minIdle:Int,
    	          testOnBorrow:Boolean,testOnReturn:Boolean,maxWaitMillis:Long):Unit = {
    	        if(pool == null){
    	          val poolConfig = new GenericObjectPoolConfig()
    	          poolConfig.setMaxIdle(maxIdle)

                  poolConfig.setMinIdle(minIdle)

                  poolConfig.setTestOnBorrow(testOnBorrow)

                  poolConfig.setTestOnReturn(testOnReturn)

                  poolConfig.setMaxWaitMillis(maxWaitMillis)

                  pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)

    	        }
    	      }
    	      
    	      def getPool : JedisPool = {
    	        pool
    	      }
    	    }
    	    
    	  // Redis configurations
          val maxTotal = 10
          val maxIdle = 10
          val minIdle = 1
          val redisHost = "localhost"
          val redisPort = 6379
          val redisTimeout = 30000
          val dbIndex = 1
          InternalRedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)
          
          val jedis =InternalRedisClient.getPool.getResource
          jedis.select(dbIndex)
          jedis.hincrBy("abc", "zhengdifei", 100)
          InternalRedisClient.getPool.returnResource(jedis)
    	  })
    	})
    	
//    	if(!rdd.isEmpty){
//    	  val t = sqc.jsonRDD(rdd)
//    	  //t.printSchema()
//    	  t.registerTempTable("eData")
//    	  val sqlReport = sqc.sql("select SNAME,count(SNAME) as num,AVG(UA) avg_ua,sum(jg) as sum_jg from eData group by SNAME order by sum_jg")
//    	  sqlReport.foreach(print)
//    	}
    })
    //ds3.print()
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}