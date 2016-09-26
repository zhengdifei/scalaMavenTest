package com.visenergy.rdd.batch

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import redis.clients.jedis.Jedis
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import redis.clients.jedis.JedisPool  
import redis.clients.jedis.JedisPoolConfig 

object QueryDataDistribution {
  def main(args:Array[String]) {
    if (args.length < 3) {
      println("Please input correct params | Usage: busiType(0 represents sales count distribution, 1 represents sales amount distribution) startTime(yyyy-MM-dd HH:mm:ss) endTime(yyyy-MM-dd HH:mm:ss) distributionNum key1 value1 key2 value2....")
      return;
    }
    
    val distributionBeginTime = System.currentTimeMillis()
    
    val busiType = args(0).toInt
    
//    val startTime = BaseConfig.strToTimeStamp(args(1))
//    val endTime = BaseConfig.strToTimeStamp(args(2))
    val startTime = args(1).toLong
    val endTime = args(2).toLong
    
    val distributionNum = args(3).toInt
    
    var pairList = List[Tuple2[String, String]]()
    for(param <- 4 to args.length/2 + 1) {
      pairList ::= (args(2 * param-4), args(2 * param-3))
    }
    
    val conf = new SparkConf().setAppName("QueryDataDistribution")
    val sc = new SparkContext(conf)
    val rddUtil:RddOpUtil = new RddOpUtil()
    
    val hbaseBeginTime = System.currentTimeMillis()
    var hbaseData = HBase2Spark.getHBaseRdd(sc, startTime, endTime)
    val hbaseEndTime = System.currentTimeMillis()
    println("hbase处理时间："+(hbaseEndTime - hbaseBeginTime)/1000d+"s")
    
    val filterBeginTime = System.currentTimeMillis()
    val filteredRdd = rddUtil.conditionFilter(hbaseData, pairList)
    val filterEndTime = System.currentTimeMillis()
    println("condition filter处理时间："+(filterEndTime - filterBeginTime)/1000d+"s")
    
    val config:JedisPoolConfig = new JedisPoolConfig()
    config.setMaxTotal(500)
    config.setMaxWaitMillis(1000 * 100)
    config.setTestOnBorrow(true)
    val redisPool = new JedisPool(config, BaseConfig.REDIS_IP, BaseConfig.REDIS_PORT)
    
    val es:ExecutorService = Executors.newCachedThreadPool()
    
    var targetKey = ""
    pairList.reverse.foreach(e => {
      targetKey = targetKey + e._1 + "_"
    })    
    
    // bytes num
    es.execute(new Runnable() {
      def run() {
        val bytesNumBeginTime = System.currentTimeMillis()
        
        val jedis = redisPool.getResource
        try {
          jedis.set(targetKey + "_bytesNum", rddUtil.getMsgBytesCount(sc, filteredRdd).toString())
          jedis.expire(targetKey + "_bytesNum", BaseConfig.REDIS_KEY_TIMEOUT)
          
          jedis.set("distribution_time", ((System.currentTimeMillis() - distributionBeginTime)/1000d).toString())
          jedis.expire("distribution_time", BaseConfig.REDIS_KEY_TIMEOUT)
        } catch {
          case ex: Exception => {
              ex.printStackTrace()
          }
        } finally {
            redisPool.returnResource(jedis)
        }
        
        val bytesNumEndTime = System.currentTimeMillis()
        println("bytes number处理时间："+(bytesNumEndTime - bytesNumBeginTime)/1000d+"s")
        
        
      }
    })
    
    es.execute(new Runnable() {
      def run() {
        val countOrAmountBeginTime = System.currentTimeMillis()
        
        val jedis = redisPool.getResource
        try {
          if (0 == busiType) {
            jedis.set(targetKey + "_count", rddUtil.getMsgCount(filteredRdd).toString())
            jedis.expire(targetKey + "_count", BaseConfig.REDIS_KEY_TIMEOUT)
          } else if (1 == busiType) {
            jedis.set(targetKey + "_amount", rddUtil.getSumByKey(filteredRdd, BaseConfig.SALES_AMOUNT_PROPERTY).toString())
            jedis.expire(targetKey + "_amount", BaseConfig.REDIS_KEY_TIMEOUT)
          }
          
          jedis.set("distribution_time", ((System.currentTimeMillis() - distributionBeginTime)/1000d).toString())
          jedis.expire("distribution_time", BaseConfig.REDIS_KEY_TIMEOUT)
        } catch {
          case ex: Exception => {
              ex.printStackTrace()
          }
        } finally {
            redisPool.returnResource(jedis)
        }
        
        val countOrAmountEndTime = System.currentTimeMillis()
        println("countOrAmount 处理时间："+(countOrAmountEndTime - countOrAmountBeginTime)/1000d+"s")
      }
    })
          
    // distribution data
    es.execute(new Runnable() {
      def run() {
        
        val distibutionBeginTime = System.currentTimeMillis()
        
        val jedis = redisPool.getResource
        
        try {
          if (0 == busiType) {
            val result = rddUtil.getCountDistributionByTimeSpan(filteredRdd, BaseConfig.DATA_DISTRIBUTION_KEY, startTime, endTime, distributionNum)
            for(i <- 1 to distributionNum) {
              var isNull = false
              result.foreach(e => {
                if (e._1 == i) {
                  isNull = true
                  jedis.hset(targetKey + "_distribution_count", (startTime + ((endTime-startTime)/distributionNum)*i).toString(), e._2.toString())
                  jedis.expire(targetKey + "_distribution_count", BaseConfig.REDIS_KEY_TIMEOUT)
                }
              })
              
              if(!isNull) {
                jedis.hset(targetKey + "_distribution_count", (startTime + ((endTime-startTime)/distributionNum)*i).toString(), "0")
                jedis.expire(targetKey + "_distribution_count", BaseConfig.REDIS_KEY_TIMEOUT)
              }
            }
          } else if (1 == busiType) {
            val result = rddUtil.getTimeSpanDistributionNew(filteredRdd, BaseConfig.DATA_DISTRIBUTION_KEY, BaseConfig.SALES_AMOUNT_PROPERTY, startTime, endTime, distributionNum)
            for(i <- 1 to distributionNum) {
              var isNull = false
              result.foreach(e => {
                if (e._1 == i) {
                  isNull = true
                  jedis.hset(targetKey + "_distribution_amount", (startTime + ((endTime-startTime)/distributionNum)*i).toString(), e._2.toString())
                  jedis.expire(targetKey + "_distribution_amount", BaseConfig.REDIS_KEY_TIMEOUT)
                }
              })
              
              if(!isNull) {
                jedis.hset(targetKey + "_distribution_amount", (startTime + ((endTime-startTime)/distributionNum)*i).toString(), "0")
                jedis.expire(targetKey + "_distribution_amount", BaseConfig.REDIS_KEY_TIMEOUT)
              }
            }
          }
          
          jedis.set("distribution_time", ((System.currentTimeMillis() - distributionBeginTime)/1000d).toString())
          jedis.expire("distribution_time", BaseConfig.REDIS_KEY_TIMEOUT)
          
        } catch {
          case ex: Exception => {
              ex.printStackTrace()
          }
        } finally {
            redisPool.returnResource(jedis)
        }
        
        val distibutionEndTime = System.currentTimeMillis()
        println("distribution 处理时间："+(distibutionEndTime - distibutionBeginTime)/1000d+"s")
      }
    })
    
    
  }
}