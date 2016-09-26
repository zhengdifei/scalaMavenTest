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

object SparkStreamingDemo {
  
  // redis连接池初始化
  val redisPool = new JedisPool({
    val config:JedisPoolConfig = new JedisPoolConfig()
    config.setMaxTotal(500)
    config.setMaxWaitMillis(1000 * 100)
    config.setTestOnBorrow(true)
    config
    }, StreamingConfig.REDIS_IP, StreamingConfig.REDIS_PORT)
  
  def main(args:Array[String]) {
    
    if (args.length != 6) {
      println("Please input correct params | Usage: key1 value1 key2 value2 key3 value3")
      return;
    }
    
    var pairList = List[Tuple2[String, String]]()
    for(param <- 0 to args.length/2 - 1) {
      pairList ::= (args(2 * param), args(2 * param+1))
    }
    
    val ssc = StreamingContext.getOrCreate(StreamingConfig.CHECKPOINT_DIR, createStreamingContext)
    
    // 从kafka消费数据
    val topics = Set("sensorData")
    val kafkaParams = Map[String,String]("metadata.broker.list" -> StreamingConfig.KAFKA_BROKER_LIST)
    val sensorDataStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topics).map(x => x._2)
    
    insertHBaseData(sensorDataStream)
    conditionFilterData(sensorDataStream, pairList, ssc)
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
    val conf = new SparkConf().setAppName("SparkStreamingTask");
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
  
  /**
   * 条件过滤数据统计
   */
  def conditionFilterData(dstream:DStream[String], pairList:List[Tuple2[String, String]], ssc:StreamingContext) {
    val condition1 = Array(pairList(0))
    val condition2 = Array(pairList(1))
    val condition3 = Array(pairList(2))
    val condition4 = Array(pairList(0), pairList(1))
    val condition5 = Array(pairList(0), pairList(2))
    val condition6 = Array(pairList(1), pairList(2))
    val condition7 = Array(pairList(0), pairList(1), pairList(2))
    
    val targetKeyArr = Array("_filter", "_" + condition1(0)._2  + "_filter", "_" + condition2(0)._2  + "_filter", 
        "_" + condition3(0)._2  + "_filter", "_" + condition4(0)._2 + "_" + condition4(1)._2 + "_filter",
        "_" + condition5(0)._2 + "_" + condition5(1)._2 + "_filter", "_" + condition6(0)._2 + "_" + condition6(1)._2 + "_filter",
        "_" + condition7(0)._2 + "_" + condition7(1)._2 + "_" + condition7(2)._2 + "_filter")
    
    val filteredDStream = dstream.transform(rdd => rdd.flatMap(jsonArrStr => {
      val jsonArr = new JSONArray(jsonArrStr)
      for(i <- 0 to jsonArr.length -1) yield jsonArr.getJSONObject(i)
    }))  
    
    val filterDStreamArr = Array(filteredDStream, filterData(filteredDStream, condition1), 
        filterData(filteredDStream, condition2), filterData(filteredDStream, condition3),
        filterData(filteredDStream, condition4), filterData(filteredDStream, condition5),
        filterData(filteredDStream, condition6), filterData(filteredDStream, condition7))
    
    for(i <- 0 to 7) {
      getFilterTotalAndRank(filterDStreamArr(i), targetKeyArr(i), ssc)
    }
  }
  
  def getFilterTotalAndRank(jsonDStream:DStream[JSONObject], targetKey:String, ssc:StreamingContext) {
    // 统计总体数据
    val mapDStream = jsonDStream.transform(rdd => rdd.map(jsonObj => {
      (1L, jsonObj.toString().getBytes.length.toLong, jsonObj.get(StreamingConfig.AMOUNT_PROPERTY).toString().toLong)
    }))
    
    val conditionFilterDStream = mapDStream.map(data => (targetKey, data))
    val initialRDD = ssc.sparkContext.parallelize(List((targetKey, (0L, 0L, 0L))))
    
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
        
    val stateDStream = conditionFilterDStream.updateStateByKey[Tuple3[Long,Long,Long]](updateTotalDataValue,
        new HashPartitioner(ssc.sparkContext.defaultParallelism), true, initialRDD)
    
    stateDStream.checkpoint(Seconds(8 * StreamingConfig.STREAMING_INTERVAL))
    
    stateDStream.foreachRDD( rdd => {
      rdd.foreach(e => {
        val jedis = redisPool.getResource
        try {
          jedis.hset(e._1, "count", e._2._1.toString())
          jedis.hset(e._1, "bytes", e._2._2.toString())
          jedis.hset(e._1, "amount", e._2._3.toString())
//            jedis.expire(e._1, StreamingConfig.STREAMING_INTERVAL * 2)
        } catch {
          case ex: Exception => {
              ex.printStackTrace()
          }
        } finally {
            redisPool.returnResource(jedis)
        }
      })
    })
    
    // 统计排行
    val brandDStream = jsonDStream.transform(rdd => rdd.map(jsonObj => {
      (jsonObj.get(StreamingConfig.BRAND_CLASSIFICATION_PROPERTY).toString(), 1, jsonObj.get(StreamingConfig.AMOUNT_PROPERTY).toString().toLong)
    }))
    val cityDStream = jsonDStream.transform(rdd => rdd.map(jsonObj => {
      (jsonObj.get(StreamingConfig.CITY_CLASSIFICATION_PROPERTY).toString(), 1, jsonObj.get(StreamingConfig.AMOUNT_PROPERTY).toString().toLong)
    }))
    
    val brandCountDStream = brandDStream.map(e => (e._1, e._2.toLong))
    val brandAmountDStream = brandDStream.map(e => (e._1, e._3.toLong))
    
    val cityCountDStream = cityDStream.map(e => (e._1, e._2.toLong))
    val cityAmountDStream = cityDStream.map(e => (e._1, e._3.toLong))
    
    val rankInitialRDD = ssc.sparkContext.parallelize(List(("", 0L)))
    
    val addDataFunc = (iterator: Iterator[(String, Seq[Long], Option[Long])]) => {
      iterator.flatMap(t => {
        val newValue = t._2.sum
        val stateValue = t._3.getOrElse(0L);
        Some(newValue + stateValue)
      }.map(sumedValue => (t._1, sumedValue)))
    }
    
    val brandCountRankDS = brandCountDStream.updateStateByKey[Long](addDataFunc,
        new HashPartitioner(ssc.sparkContext.defaultParallelism), true, rankInitialRDD)
    val brandAmountRankDS = brandAmountDStream.updateStateByKey[Long](addDataFunc,
        new HashPartitioner(ssc.sparkContext.defaultParallelism), true, rankInitialRDD)
    val cityCountRankDS = cityCountDStream.updateStateByKey[Long](addDataFunc,
        new HashPartitioner(ssc.sparkContext.defaultParallelism), true, rankInitialRDD)
    val cityAmountRankDS = cityAmountDStream.updateStateByKey[Long](addDataFunc,
        new HashPartitioner(ssc.sparkContext.defaultParallelism), true, rankInitialRDD)
    
    
    brandCountRankDS.checkpoint(Seconds(10 * StreamingConfig.STREAMING_INTERVAL)) 
    brandAmountRankDS.checkpoint(Seconds(10 * StreamingConfig.STREAMING_INTERVAL)) 
    cityCountRankDS.checkpoint(Seconds(10 * StreamingConfig.STREAMING_INTERVAL)) 
    cityAmountRankDS.checkpoint(Seconds(10 * StreamingConfig.STREAMING_INTERVAL)) 
    
    brandCountRankDS.foreachRDD( rdd => {
      rdd.foreach(e => {
        val jedis = redisPool.getResource
        try {
          jedis.hset(targetKey + "_realtime_brand_count_rank", e._1, e._2.toString())
        } catch {
          case ex: Exception => {
              ex.printStackTrace()
          }
        } finally {
            redisPool.returnResource(jedis)
        }
      })
    })
    
    brandAmountRankDS.foreachRDD( rdd => {
      rdd.foreach(e => {
        val jedis = redisPool.getResource
        try {
          jedis.hset(targetKey + "_realtime_brand_amount_rank", e._1, e._2.toString())
        } catch {
          case ex: Exception => {
              ex.printStackTrace()
          }
        } finally {
            redisPool.returnResource(jedis)
        }
      })
    })  
    
    cityCountRankDS.foreachRDD( rdd => {
      rdd.foreach(e => {
        val jedis = redisPool.getResource
        try {
          jedis.hset(targetKey + "_realtime_city_count_rank", e._1, e._2.toString())
        } catch {
          case ex: Exception => {
              ex.printStackTrace()
          }
        } finally {
            redisPool.returnResource(jedis)
        }
      })
    })
    
    cityAmountRankDS.foreachRDD( rdd => {
      rdd.foreach(e => {
        val jedis = redisPool.getResource
        try {
          jedis.hset(targetKey + "_realtime_city_amount_rank", e._1, e._2.toString())
        } catch {
          case ex: Exception => {
              ex.printStackTrace()
          }
        } finally {
            redisPool.returnResource(jedis)
        }
      })
    })
  }
  
  def filterData(inputRdd : DStream[JSONObject], pairList:Array[(String, String)]) : DStream[JSONObject] = {
    inputRdd.transform(rdd => rdd.filter(jsonObj => {
      var result = true
      for(tuple <- pairList) {
        result = result && (jsonObj.get(tuple._1).toString() == tuple._2)
      }
      result
    }))
  }
  
  /**
   * 窗口操作
   */
  def handleWindowData(dstream:DStream[String]) {
    val jsonDStream = dstream.transform(rdd => rdd.flatMap(jsonArrStr => {
      val jsonArr = new JSONArray(jsonArrStr)
      for(i <- 0 to jsonArr.length -1) yield jsonArr.getJSONObject(i)
    }))
    
    // 统计近五分钟的字节数，总数，销售额
    val bytesWindowDStream = dstream.map(jsonArrStr => jsonArrStr.getBytes.length).reduceByWindow(
        _ + _, _ - _, Seconds(50 * StreamingConfig.STREAMING_INTERVAL), Seconds(StreamingConfig.STREAMING_INTERVAL))
    val countWindowDStream = jsonDStream.map(jsonObj => 1L).reduceByWindow(
        _ + _, _ - _, Seconds(50 * StreamingConfig.STREAMING_INTERVAL), Seconds(StreamingConfig.STREAMING_INTERVAL))
    val amountWindowDStream = jsonDStream.map(jsonObj => jsonObj.get(StreamingConfig.AMOUNT_PROPERTY).toString().toLong).reduceByWindow(
        _ + _, _ - _, Seconds(50 * StreamingConfig.STREAMING_INTERVAL), Seconds(StreamingConfig.STREAMING_INTERVAL))
    
    bytesWindowDStream.foreachRDD(rdd => {
      rdd.foreach(bytesNumber => {
        val jedis = redisPool.getResource
        try {
          jedis.hset("minutes_window", "bytes", bytesNumber.toString())
          jedis.hset("five_minutes_bytes", System.currentTimeMillis().toString(), bytesNumber.toString())
        } catch {
          case ex: Exception => {
              ex.printStackTrace()
          }
        } finally {
            redisPool.returnResource(jedis)
        }
      })
    })
    
    countWindowDStream.foreachRDD(rdd => {
      rdd.foreach(count => {
        val jedis = redisPool.getResource
        try {
          jedis.hset("minutes_window", "count", count.toString())
        } catch {
          case ex: Exception => {
              ex.printStackTrace()
          }
        } finally {
            redisPool.returnResource(jedis)
        }
      })
    })
  
    amountWindowDStream.foreachRDD(rdd => {
      rdd.foreach(amount => {
        val jedis = redisPool.getResource
        try {
          jedis.hset("minutes_window", "amount", amount.toString())
        } catch {
          case ex: Exception => {
              ex.printStackTrace()
          }
        } finally {
            redisPool.returnResource(jedis)
        }
      })
    })
    
    val windowStream = jsonDStream.window(Seconds(50 * StreamingConfig.STREAMING_INTERVAL), Seconds(StreamingConfig.STREAMING_INTERVAL)) 
    
    windowStream.foreachRDD(rdd => {
      val jedis = redisPool.getResource
      try {
        jedis.hset("minutes_window", "median_price", getMedianNumber(rdd, StreamingConfig.AMOUNT_PROPERTY).toString())
      } catch {
        case ex: Exception => {
            ex.printStackTrace()
        }
      } finally {
          redisPool.returnResource(jedis)
      }
    })
    
  }
  
  def getMedianNumber(input:RDD[JSONObject], key:String) : Long = {
    val msgCount = input.count()
    val mapRdd = input.map(jsonObj => jsonObj.get(key).toString().toLong)
    val msgPos = ((msgCount + 1) * 0.5).toLong
    partitionRdd(mapRdd, msgPos)
  }
  
  def partitionRdd(input:RDD[Long], msgPos:Long) : Long = {
    input.coalesce(1).sortBy(x=>x, true).mapPartitions { 
      iter => {
         var result = List[Long]()
         var pos = 1L
		     while (iter.hasNext) {
		       val number = iter.next()
		       if (pos == msgPos) {
		         result ::= number
		       }
		       pos += 1
		     }
    	   result.iterator
      }
    }.collect().apply(0)
  }
  
}