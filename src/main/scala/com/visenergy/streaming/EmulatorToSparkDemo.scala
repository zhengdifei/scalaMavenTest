package com.visenergy.streaming

import org.apache.spark.streaming.dstream.DStream
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.client.Put
import org.json.JSONArray
import org.json.JSONObject
import org.apache.spark.HashPartitioner

object EmulatorToSparkDemo {
  
  // redis连接池初始化
  val redisPool = new JedisPool({
    val config:JedisPoolConfig = new JedisPoolConfig()
    config.setMaxTotal(500)
    config.setMaxWaitMillis(1000 * 100)
    config.setTestOnBorrow(true)
    config
    }, StreamingConfig.REDIS_IP, StreamingConfig.REDIS_PORT)
  
  def main(args:Array[String]) {
    val ssc = StreamingContext.getOrCreate(StreamingConfig.CHECKPOINT_DIR, createStreamingContext)
    
    // 从kafka消费数据
    val topics = Set("sensorData")
    val kafkaParams = Map[String,String]("metadata.broker.list" -> StreamingConfig.KAFKA_BROKER_LIST)
    val sensorDataStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topics).map(x => x._2)
    
    insertHBaseData(sensorDataStream)
    handleIncreaseTotalData(sensorDataStream, ssc)
    handleWindowData(sensorDataStream)
    
    ssc.start
    ssc.awaitTermination
  }
  
  /**
   * 将rdd内对象，以json数组字符串形式存储rowkey为时间构造hbase存储单元
   */
  def hbaseAdaptor(currentTime:Long,jsonArrayStr:String) = {
    try{
		  val jsonArray = new JSONArray(jsonArrayStr)
	    val put = new Put(Bytes.toBytes(currentTime.toString))
  		for(i <- 0 to jsonArray.length -1) {
  		  val jsonObj = jsonArray.getJSONObject(i)
  		  put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(jsonObj.getString("SID")+"_"+i), Bytes.toBytes(jsonObj.toString()))
  		}
	  	(new ImmutableBytesWritable, put)
    } catch {
    	case ex: Exception => {
    		println("create Hbase Object error")
    		null
      }
    }
  }
  
  /**
   * 创建checkpoint恢复
   */
  def createStreamingContext():StreamingContext = {
    val conf = new SparkConf().setAppName("SparkStreamingTask").setMaster("local[2]");
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf, Seconds(StreamingConfig.STREAMING_INTERVAL));
    ssc.checkpoint(StreamingConfig.CHECKPOINT_DIR)
    ssc
  }
  
  /**
   * 往hbase写数据
   */
  def insertHBaseData(dstream:DStream[String]) {
    dstream.foreachRDD { rdd => {
        if (!rdd.isEmpty) {
          try {
	          val conf = HBaseConfiguration.create() 
            conf.set("hbase.zookeeper.quorum",StreamingConfig.HBASE_ZK_IP)
            conf.set("hbase.zookeeper.property.clientPort", StreamingConfig.HBASE_ZK_PORT.toString())
            
            val jobConf = new JobConf(conf, this.getClass())
  		      jobConf.setOutputFormat(classOf[TableOutputFormat])
  		      jobConf.set(TableOutputFormat.OUTPUT_TABLE, StreamingConfig.HBASE_TABLE)
  		      
  		      // 插入数据到hbase
  		      rdd.map(hbaseAdaptor(System.currentTimeMillis(), _)).saveAsHadoopDataset(jobConf)
          } catch {
            case ex: Exception => {
              println("create Hbase connect error")
            }
          }
        }
      }
    }
  }
  
  /**
   * 数据统计
   */
  def handleIncreaseTotalData(dstream:DStream[String], ssc: StreamingContext) {
    
    // 分类数据统计
    val totalIncreaseCityDStream = dstream.transform(rdd => rdd.flatMap(jsonArrStr => {
      val jsonArr = new JSONArray(jsonArrStr)
      val totalCityData = {
        for(i <- 0 to jsonArr.length -1) yield {
         val jsonObj = jsonArr.getJSONObject(i)
         val city = jsonObj.get(StreamingConfig.CITY_CLASSIFICATION_PROPERTY).toString()
         (city, jsonObj.toString().getBytes().length.toLong, 1L)
        } 
      }
      totalCityData 
    }))
    
    val increaseBytes4City = totalIncreaseCityDStream.map(data => (data._1, data._2))
    val increaseNum4City = totalIncreaseCityDStream.map(data => (data._1, data._3))
    
    val addDataFunc = (iterator: Iterator[(String, Seq[Long], Option[Long])]) => {
      iterator.flatMap(t => {
        val newValue = t._2.sum
        val stateValue = t._3.getOrElse(0L);
        Some(newValue + stateValue)
      }.map(sumedValue => (t._1, sumedValue)))
    }
     
    val cityInitialRDD = ssc.sparkContext.parallelize(List(("", 0L)))
    val cityBytesDStream = increaseBytes4City.updateStateByKey[Long](addDataFunc, 
        new HashPartitioner(ssc.sparkContext.defaultParallelism), true, cityInitialRDD)
    val cityNumDStream = increaseNum4City.updateStateByKey[Long](addDataFunc, 
        new HashPartitioner(ssc.sparkContext.defaultParallelism), true, cityInitialRDD)
    
    cityBytesDStream.checkpoint(Seconds(10 * StreamingConfig.STREAMING_INTERVAL)) 
    cityNumDStream.checkpoint(Seconds(10 * StreamingConfig.STREAMING_INTERVAL)) 
    
    cityBytesDStream.foreachRDD( rdd => {
      if(!rdd.isEmpty) {
        rdd.foreach(e => {
          val jedis = redisPool.getResource
          try {
            jedis.hset("increase_city_bytes", e._1, e._2.toString())
          } catch {
            case ex: Exception => {
                ex.printStackTrace()
            }
          } finally {
              redisPool.returnResource(jedis)
          }
        })
      }
    })
    
    cityNumDStream.foreachRDD( rdd => {
      if(!rdd.isEmpty) {
        rdd.foreach(e => {
          val jedis = redisPool.getResource
          try {
            jedis.hset("increase_city_num", e._1, e._2.toString())
          } catch {
            case ex: Exception => {
                ex.printStackTrace()
            }
          } finally {
              redisPool.returnResource(jedis)
          }
        })
      }
    })
      
    // 总体数据统计
    val totalDataDStream = dstream.transform(rdd => rdd.map(jsonArrStr => {
       val jsonArr = new JSONArray(jsonArrStr)
       (jsonArr.length().toLong, jsonArrStr.getBytes.size.toLong, {
         var salesAmount = 0L
         for(i <- 0 to jsonArr.length -1) {
           salesAmount = salesAmount + jsonArr.getJSONObject(i).get(StreamingConfig.AMOUNT_PROPERTY).toString().toLong
         }
         salesAmount
       })
    }))
    
    val totalCountDStream = totalDataDStream.map(data => ("total_data", data))
    val initialRDD = ssc.sparkContext.parallelize(List(("total_data", (0L, 0L, 0L))))
    
    val updateTotalDataValue = (iterator: Iterator[(String, Seq[Tuple3[Long,Long,Long]],
        Option[Tuple3[Long,Long,Long]])]) => {
          iterator.flatMap(t => {
            var eCount = 0L
            var eBytes = 0L
            var eAmount = 0L
            val newValue = t._2.foreach(e => {
              eCount += e._1
              eBytes += e._2
              eAmount += e._3
            })
            
            val stateValue = t._3.getOrElse((0L, 0L, 0L));
            Some(eCount + stateValue._1, eBytes + stateValue._2, eAmount + stateValue._3)
      }.map(sumedValue => (t._1, sumedValue)))
    }
        
    val stateDStream = totalCountDStream.updateStateByKey[Tuple3[Long,Long,Long]](updateTotalDataValue,
        new HashPartitioner(ssc.sparkContext.defaultParallelism), true, initialRDD)
    
    stateDStream.checkpoint(Seconds(8 * StreamingConfig.STREAMING_INTERVAL))  
    
    stateDStream.foreachRDD( rdd => {
      if(!rdd.isEmpty) {
        rdd.foreach(e => {
          val jedis = redisPool.getResource
          try {
            jedis.hset("realtime_total_data", "count", e._2._1.toString())
            jedis.hset("realtime_total_data", "bytes", e._2._2.toString())
            jedis.hset("realtime_total_data", "amount", e._2._3.toString())
          } catch {
            case ex: Exception => {
                ex.printStackTrace()
            }
          } finally {
              redisPool.returnResource(jedis)
          }
        })
      }
    })
  }
  
  /**
   * 窗口操作
   */
  def handleWindowData(dstream:DStream[String]) {
    val windowDStream = dstream.map(jsonArrStr => jsonArrStr.getBytes.length).reduceByWindow(
        _ + _, _ - _, Seconds(50 * StreamingConfig.STREAMING_INTERVAL), Seconds(5 * StreamingConfig.STREAMING_INTERVAL))
    
    windowDStream.foreachRDD(rdd => {
      if(!rdd.isEmpty) {
        rdd.foreach(bytesNumber => {
          val jedis = redisPool.getResource
          try {
            jedis.hset("five_minutes_bytes", System.currentTimeMillis().toString(), bytesNumber.toString())
//            jedis.expire("five_minutes_bytes", StreamingConfig.STREAMING_INTERVAL * 100)
          } catch {
            case ex: Exception => {
                ex.printStackTrace()
            }
          } finally {
              redisPool.returnResource(jedis)
          }
        })
      }
    })
  }
}