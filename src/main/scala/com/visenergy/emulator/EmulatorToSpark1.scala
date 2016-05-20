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
/**
 * hbase + redis + kafka + streaming + spark Sql整体解决方案
 * hbase存储方式SID作为rowkey，每个属性一个列族
 */
object EmulatorToSpark1 {
  //将json数组字符串变成json对象字符串
  def parseJsonArrStr2JsonObjStr(jsonArrayStr:String):Array[String] = {
    val transformOne = jsonArrayStr.replace("[", "").replace("]", "").replace("},{", "}&&{")
    transformOne.split("&&")
  }
  
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
  //将普通对象转成hbase存储对象
  def hbaseAdaptor(jsonObjStr:String) = {
    val jsonObj = new JSONObject(jsonObjStr)
    val p = new Put(Bytes.toBytes(jsonObj.getString("SID")))
	p.addColumn(Bytes.toBytes("SNAME"), Bytes.toBytes("name"), Bytes.toBytes(jsonObj.getString("SNAME")))
  	p.addColumn(Bytes.toBytes("UA"), Bytes.toBytes("name"), Bytes.toBytes(jsonObj.getDouble("UA")))
  	p.addColumn(Bytes.toBytes("IA"), Bytes.toBytes("name"), Bytes.toBytes(jsonObj.getDouble("IA")))

  	(new ImmutableBytesWritable,p)
  }
  //将rdd内对象，以json字符串形式存储rowkey为时间
  def hbaseAdaptor2(currentTime:Long,rddStr:String) = {
    println(rddStr)
    val jsonObj = new JSONObject(rddStr)
    val p = new Put(Bytes.toBytes(currentTime))
	p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(jsonObj.getString("SID")), Bytes.toBytes(rddStr))

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
    
    val sensorDs = sensorDataStreaming.map(x => x._2).flatMap(parseJsonArrStr2JsonObjStr)
    sensorDs.foreachRDD{ rdd => 
      if(!rdd.isEmpty){
//        val df = sqlc.jsonRDD(rdd)
//	    df.registerTempTable("sensorDf")
//    	val sqlReport = sqlc.sql("select SNAME,count(SNAME) as num,AVG(UA) avg_ua,sum(jg) as sum_jg from sensorDf group by SNAME order by sum_jg")
//    	//数据处理
//	    RedisDao.makePool()
//		val redisClient = RedisDao.getPool.getResource()
//    	sqlReport.foreach(one => {
//    		redisClient.hincrBy("sensor_num", one.getAs[String]("SNAME"), one.getAs[Long]("num"))
//    		redisClient.hset("avg_ua", one.getAs[String]("SNAME"), one.getAs[Double]("avg_ua").toString)
//    		redisClient.hset("sum_jg", one.getAs[String]("SNAME"),  one.getAs[Long]("sum_jg").toString)
//    	})
//    	RedisDao.getPool.returnResource(redisClient)
	    //数据存入hbase
        val currentTime = System.currentTimeMillis()
	    rdd.map(hbaseAdaptor2(currentTime,_))saveAsHadoopDataset(getJobConf)
	    
      }
    }
    
    ssc.start()
    ssc.awaitTermination()
  }

}