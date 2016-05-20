package com.visenergy.emulator

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
import org.json.JSONObject
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD
import org.json.JSONArray
import org.apache.spark.Accumulator
/**
 * 数据以时间为rowkey存入hbase
 */
object EmulatorToSpark2 {
  
  //获取hbase任务配置
  def getJobConf():JobConf = {
    val conf = HBaseConfiguration.create()
	conf.set("hbase.zookeeper.quorum","localhost")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    
    val jobConf = new JobConf(conf,this.getClass())
	jobConf.setOutputFormat(classOf[TableOutputFormat])
	jobConf.set(TableOutputFormat.OUTPUT_TABLE,"sensorData")
	
	jobConf
  }
  //将rdd内对象，以json数组字符串形式存储rowkey为时间
  def hbaseAdaptor(currentTime:Long,jsonArrayStr:String) = {
	val jsonArray = new JSONArray(jsonArrayStr)
    val p = new Put(Bytes.toBytes(currentTime))
	for(i <- 0 to jsonArray.length -1){
	  val jsonObj = jsonArray.getJSONObject(i)
	  p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(jsonObj.getString("SID")+"_"+i), Bytes.toBytes(jsonObj.toString()))
	}
  	(new ImmutableBytesWritable,p)
  }
  
  def main(args: Array[String]): Unit = {
    //sparkStreaming 初始化
    val sparkConf = new SparkConf().setAppName("EmulatorToSpark").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(6))
    val sqlc = new SQLContext(sc)
    
    //初始化kafka direct kafka环境
    val topics = Set("sensorData")
    val kafkaParams = Map[String,String]("metadata.broker.list" -> "localhost:9092")
    val sensorDataStreaming = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topics)
    
    val sensorDs = sensorDataStreaming.map(x => x._2)
    sensorDs.foreachRDD{ rdd => 
      if(!rdd.isEmpty){
    	//将数据存以时间作为rowkey，存入hbase
        val currentTime = System.currentTimeMillis()
        rdd.foreach(println)
	    rdd.map(hbaseAdaptor(currentTime,_))saveAsHadoopDataset(getJobConf)
      }
    }
    sensorDs.print()
    ssc.start()
    ssc.awaitTermination()
  }

}