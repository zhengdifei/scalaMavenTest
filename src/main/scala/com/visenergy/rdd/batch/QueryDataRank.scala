package com.visenergy.rdd.batch

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService

object QueryDataRank {
  def main(args:Array[String]) {
    if (args.length != 4) {
      println("Please input correct params | Usage: startTime(yyyy-MM-dd HH:mm:ss) endTime(yyyy-MM-dd HH:mm:ss) property topN")
      return
    }
    
    println("Begin to QueryDataRank..............")
    
    val rankBeginTime = System.currentTimeMillis()
    
//    val startTime = BaseConfig.strToTimeStamp(args(0))
//    val endTime = BaseConfig.strToTimeStamp(args(1))
    val startTime = args(0).toLong
    val endTime = args(1).toLong
    val property = args(2)
    val topN = args(3).toInt
    
    val conf = new SparkConf().setAppName("QueryDataRank")
    val sc = new SparkContext(conf)
    
    val rddUtil:RddOpUtil = new RddOpUtil()
    
    val hbaseBeginTime = System.currentTimeMillis()
    var hbaseData = HBase2Spark.getHBaseRdd(sc, startTime, endTime)
    val hbaseEndTime = System.currentTimeMillis()
    println("hbase处理时间："+(hbaseEndTime - hbaseBeginTime)/1000d+"s")
    
    val config:JedisPoolConfig = new JedisPoolConfig()
    config.setMaxTotal(500)
    config.setMaxWaitMillis(1000 * 100)
    config.setTestOnBorrow(true)
    val redisPool = new JedisPool(config, BaseConfig.REDIS_IP, BaseConfig.REDIS_PORT) 
    
    val es:ExecutorService = Executors.newCachedThreadPool()
          
    // rank sales count 
    es.execute(new Runnable() {
      def run() {
        val salesCountRankBeginTime = System.currentTimeMillis()
        
        val jedis = redisPool.getResource
        try {
          val rankResult = rddUtil.getMsgRank(hbaseData, property).take(topN)
          val rankKey = "rank_count_" + property
          for(tuple <- rankResult) {
            jedis.hset(rankKey, tuple._1.toString(), tuple._2.toString())
            jedis.expire(rankKey, BaseConfig.REDIS_KEY_TIMEOUT)
          }
          
          jedis.set("rank_time", ((System.currentTimeMillis() - rankBeginTime)/1000d).toString())
          jedis.expire("rank_time", BaseConfig.REDIS_KEY_TIMEOUT)
        } catch {
          case ex: Exception => {
              ex.printStackTrace()
          }
        } finally {
            redisPool.returnResource(jedis)
        }
        
        val salesCountRankEndTime = System.currentTimeMillis()
        println("salesCountRank处理时间："+(salesCountRankEndTime - salesCountRankBeginTime)/1000d+"s")
      }
    })
          
    // rank sales amount
    es.execute(new Runnable() {
      def run() {
        val salesAmountRankBeginTime = System.currentTimeMillis()
        
        val jedis = redisPool.getResource
        try {
          val rankResult = rddUtil.getMsgRank4TargetKey(hbaseData, property, BaseConfig.SALES_AMOUNT_PROPERTY).take(topN)
          val rankKey = "rank_amount_" + property
          for(tuple <- rankResult) {
            jedis.hset(rankKey, tuple._1.toString(), tuple._2.toString())
            jedis.expire(rankKey, BaseConfig.REDIS_KEY_TIMEOUT)
          }
          
          jedis.set("rank_time", ((System.currentTimeMillis() - rankBeginTime)/1000d).toString())
          jedis.expire("rank_time", BaseConfig.REDIS_KEY_TIMEOUT)
        } catch {
          case ex: Exception => {
              ex.printStackTrace()
          }
        } finally {
            redisPool.returnResource(jedis)
        }
        
        val salesAmountRankEndTime = System.currentTimeMillis()
        println("salesAmountRank处理时间："+(salesAmountRankEndTime - salesAmountRankBeginTime)/1000d+"s")
      }
    })
    
    println("Finished QueryDataRank!!!!!!!!!")
    
  }
}