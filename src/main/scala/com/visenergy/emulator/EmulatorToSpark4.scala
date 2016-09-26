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
 * 在EumlatorToSpark3的基础上，计算每个模拟器消息数，及消息大小
 */
object EmulatorToSpark4 {
  //将json数组字符串变成json对象字符串
  def parseJsonArrStr2JsonObjStr(jsonArrayStr:String):Array[String] = {
    val transformOne = jsonArrayStr.replace("[", "").replace("]", "").replace("},{", "}&&{")
    transformOne.split("&&")
  }
  
  def parseJsonStr2JsonObj(jsonStr:String):JSONObject = {
    new JSONObject(jsonStr)
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
  //将rdd内对象，以json数组字符串形式存储rowkey为时间
  def hbaseAdaptor(acc_num:Accumulator[Int],acc_bytes:Accumulator[Long],currentTime:Long,jsonArrayStr:String) = {
    //println("r1:" + acc)//调试累计器进入时值
	val jsonArray = new JSONArray(jsonArrayStr)
    val p = new Put(Bytes.toBytes(currentTime))
	for(i <- 0 to jsonArray.length -1){
	  val jsonObj = jsonArray.getJSONObject(i)
	  p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(jsonObj.getString("SID")+"_"+i), Bytes.toBytes(jsonObj.toString()))
	}
	acc_num += jsonArray.length
	acc_bytes += jsonArrayStr.getBytes().length
	//println("r2:" + acc)
  	(new ImmutableBytesWritable,p)
  }
  
  def main(args: Array[String]): Unit = {
    //sparkStreaming 初始化
    val sparkConf = new SparkConf().setAppName("EmulatorToSpark").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(6))
    val sqlc = new SQLContext(sc)
    
    ssc.checkpoint("test/point")
    //初始化kafka direct kafka环境
    val topics = Set("sensorData")
    val kafkaParams = Map[String,String]("metadata.broker.list" -> "localhost:9092")
    val sensorDataStreaming = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topics)
    
    val sensorDs = sensorDataStreaming.map(x => x._2)
    //累加器内部原理：能够在rdd内部共享遍历，一个每个partition内部，都是从0开始分发，rdd之间是串行执行的，在rdd外部，是将时间轴上的累加值再进行累加
    val acc_sum = sc.accumulator(0,"sensor_sum")
    val acc_bytes = sc.accumulator(0L,"sensor_bytes")
    //foreacheRDD不是遍历rdd操作，后面的方法默认传入rdd参数，可以对RDD进行操作
    sensorDs.foreachRDD{ rdd => 
      if(!rdd.isEmpty){
	    //数据存入hbase
        val currentTime = System.currentTimeMillis()
	    rdd.map(hbaseAdaptor(acc_sum,acc_bytes,currentTime,_))saveAsHadoopDataset(getJobConf)
	    //println("d:" + acc1)//调试累计器总值
	    //用累加器统计消息模拟器发出数据总条数
	    RedisDao.makePool()
		val redisClient = RedisDao.getPool.getResource()
		redisClient.incrBy("sensor_num", acc_sum.value)
		redisClient.incrBy("sensor_bytes", acc_bytes.value)
    	RedisDao.getPool.returnResource(redisClient)
      }
    }
    //Dstream计算每个模拟器的数量
    val sensorDs2 = sensorDs.flatMap(parseJsonArrStr2JsonObjStr).map(parseJsonStr2JsonObj)
    val sensorDs3 = sensorDs2.map(rdd => (rdd.getString("SID"),1))
    val sensorDs4 = sensorDs3.reduceByKey(_+_) 
    sensorDs4.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        RedisDao.makePool()
        val redisClient = RedisDao.getPool.getResource()
        while(iter.hasNext){
          val oneSensor = iter.next
          println(oneSensor._1 )
          redisClient.hincrBy("one_sensor_num",oneSensor._1 ,oneSensor._2)
        }
    	RedisDao.getPool.returnResource(redisClient)
      })
    })
    
    //利用window，计算一段时间销售排行
    val sensorWindow1 = sensorDs2.map(rdd => (rdd.getString("SID"),rdd.getLong("jg")))
    val sensorWindow2 = sensorWindow1.reduceByKeyAndWindow(_+_, _-_, Seconds(30), Seconds(12))
    sensorWindow2.print()
    sensorWindow2.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        RedisDao.makePool()
        val redisClient = RedisDao.getPool.getResource()
        while(iter.hasNext){
          val oneSensor = iter.next
          redisClient.hset("all_sensor_jg", oneSensor._1, oneSensor._2.toString)
        }
    	RedisDao.getPool.returnResource(redisClient)
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }

}