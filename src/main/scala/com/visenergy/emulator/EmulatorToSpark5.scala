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
 * 在EumlatorToSpark4的基础上，用SparkSQL进行计算
 */
object EmulatorToSpark5 {
  //将json数组字符串变成json对象字符串
  def parseJsonArrStr2JsonObjStr(jsonArrayStr:String):Array[String] = {
    val transformOne = jsonArrayStr.replace("[", "").replace("]", "").replace("},{", "}&&{")
    transformOne.split("&&")
  }
  
  def parseJsonStr2JsonObj(jsonStr:String):JSONObject = {
    try{
    	new JSONObject(jsonStr)
    }catch{
      case ex: Exception => {
        println("JsonStr change to JsonObject error")
        //发送异常则，返回一个空JSON对象
        new JSONObject()
      }
    }
  }
  
  //获取hbase任务配置
  def getJobConf():JobConf = {
    try{
	    val conf = HBaseConfiguration.create()
		conf.set("hbase.zookeeper.quorum","localhost")
	    conf.set("hbase.zookeeper.property.clientPort", "2181")
	    
	    val jobConf = new JobConf(conf,this.getClass())
		jobConf.setOutputFormat(classOf[TableOutputFormat])
		jobConf.set(TableOutputFormat.OUTPUT_TABLE,"sensorData")
		
		jobConf
    }catch{
      case ex: Exception => {
        println("create Hbase connect error")
        null
      }
    }
  }
  
  //将rdd内对象，以json数组字符串形式存储rowkey为时间
  def hbaseAdaptor(acc_num:Accumulator[Int],acc_bytes:Accumulator[Long],currentTime:Long,jsonArrayStr:String) = {
    try{
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
    }catch{
    	case ex: Exception => {
    		println("create Hbase Object error")
    		null
      }
    }
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
	    try{
		    //数据存入hbase
	        val currentTime = System.currentTimeMillis()
		    rdd.map(hbaseAdaptor(acc_sum,acc_bytes,currentTime,_))saveAsHadoopDataset(getJobConf)
		    //println("d:" + acc1)//调试累计器总值
		    //用累加器统计消息模拟器发出数据总条数
			//初始化redis
		    RedisDao.makePool()
			val redisClient = RedisDao.getPool.getResource()
		    //统计拦截器数量，字节数
			redisClient.incrBy("sensor_num", acc_sum.value)
			redisClient.incrBy("sensor_bytes", acc_bytes.value)
	    	
			//统计其他信息
			val df1 = sqlc.jsonRDD(rdd.flatMap(parseJsonArrStr2JsonObjStr))
			df1.registerTempTable("emulator")
			val df2 = sqlc.sql("select SID,count(SID) as num,avg(jg) avg_jg,sum(jg) as sum_jg from emulator group by SID order by sum_jg")
			//因为数据量不大，所以将所有数据都汇总，不再每一个partition中创建redis
			df2.collect.foreach(row => {
			    //计算每一个模拟器数量，总价格，平均价格
				redisClient.hincrBy("one_sensor_num",row.getAs[String]("SID") ,row.getAs[Long]("num"))
				redisClient.hset("sensor_sum_jg", row.getAs[String]("SID") , row.getAs[Long]("sum_jg").toString)
				redisClient.hset("sensor_avg_jg", row.getAs[String]("SID") , row.getAs[Long]("avg_jg").toString)
			})
			RedisDao.getPool.returnResource(redisClient)
	    }catch{
		    case ex: Exception => {
	    		println("foreach rdd error")
	    		null
		    }
	    }
      }
    }
    
    //利用window，计算一段时间销售排行
    val sensorWin1 = sensorDs.flatMap(parseJsonArrStr2JsonObjStr).map(parseJsonStr2JsonObj).map(rdd => (rdd.getString("SID"),rdd.getLong("jg")))
    val sensorWin2 = sensorWin1.reduceByKeyAndWindow(_+_, _-_, Seconds(30), Seconds(12))
    sensorWin2.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        RedisDao.makePool()
        val redisClient = RedisDao.getPool.getResource()
        while(iter.hasNext){
          val oneSensor = iter.next
          redisClient.hset("all_sensor_30_jg", oneSensor._1, oneSensor._2.toString)
        }
    	RedisDao.getPool.returnResource(redisClient)
      })
    })
    
    ssc.start()
    ssc.awaitTermination()
  }

}