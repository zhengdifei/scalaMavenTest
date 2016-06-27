package com.visenergy.rdd.batch

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import redis.clients.jedis.Jedis
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import redis.clients.jedis.JedisPool  
import redis.clients.jedis.JedisPoolConfig 

object QueryTotalMsgInfo {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("QueryTotalMsgInfo")
    val sc = new SparkContext(conf)
    doQuery(sc);
  }
  
  def doQuery(sc:SparkContext) {
    println("Begin to QueryTotalMsgInfo..............")
    
    val rddUtil:RddOpUtil = new RddOpUtil()
    
    val hbaseBeginTime = System.currentTimeMillis()
    val hbaseData = HBase2Spark.getHBaseRdd(sc, 0L, System.currentTimeMillis())
    val hbaseEndTime = System.currentTimeMillis()
    println("hbase处理时间："+(hbaseEndTime - hbaseBeginTime)/1000d+"s")
    
    
    var config:JedisPoolConfig = new JedisPoolConfig()
    config.setMaxTotal(500);
    config.setMaxWaitMillis(1000 * 100);
    config.setTestOnBorrow(true);
    val redisPool = new JedisPool(config, BaseConfig.REDIS_IP, BaseConfig.REDIS_PORT); 
    
    val es:ExecutorService = Executors.newCachedThreadPool()
          
    // sales_count 
    es.execute(new Runnable() {
      def run() {
        val rddBeginTime = System.currentTimeMillis()
        val jedis = redisPool.getResource
        try {
          jedis.hset("total_figure", "sales_count", rddUtil.getMsgCount(hbaseData).toString())
        } catch {
          case ex: Exception => {
              ex.printStackTrace()
          }
        } finally {
            redisPool.returnResource(jedis)
        }
        val rddEndTime = System.currentTimeMillis()
        println("rdd batch处理时间："+(rddEndTime - rddBeginTime)/1000d+"s")
      }
    })
          
    // sales_amount
    es.execute(new Runnable() {
      def run() {
        val rddBeginTime = System.currentTimeMillis()
        val jedis = redisPool.getResource
        try {
          jedis.hset("total_figure", "sales_amount", rddUtil.getSumByKey(hbaseData, BaseConfig.SALES_AMOUNT_PROPERTY).toString())
        } catch {
          case ex: Exception => {
              ex.printStackTrace()
          }
        } finally {
            redisPool.returnResource(jedis)
        }
        val rddEndTime = System.currentTimeMillis()
        println("rdd batch处理时间："+(rddEndTime - rddBeginTime)/1000d+"s")
      }
    })
          
    // mid_price
    es.execute(new Runnable() {
      def run() {
        val rddBeginTime = System.currentTimeMillis()
        val jedis = redisPool.getResource
        try {
          jedis.hset("total_figure", "mid_price", rddUtil.getSectionNum(hbaseData, BaseConfig.SALES_AMOUNT_PROPERTY, 0.5).toString())
        } catch {
          case ex: Exception => {
              ex.printStackTrace()
          }
        } finally {
            redisPool.returnResource(jedis)
        }
        val rddEndTime = System.currentTimeMillis()
        println("rdd batch处理时间："+(rddEndTime - rddBeginTime)/1000d+"s")
      }
    })
          
    // avg_price
    es.execute(new Runnable() {
      def run() {
        val rddBeginTime = System.currentTimeMillis()
        val jedis = redisPool.getResource
        try {
          jedis.hset("total_figure", "avg_price", rddUtil.getAvgByKey(hbaseData, BaseConfig.SALES_AMOUNT_PROPERTY).toString())
        } catch {
          case ex: Exception => {
              ex.printStackTrace()
          }
        } finally {
            redisPool.returnResource(jedis)
        }
        val rddEndTime = System.currentTimeMillis()
        println("rdd batch处理时间："+(rddEndTime - rddBeginTime)/1000d+"s")
      }
    })
    
    // total bytes sum
    es.execute(new Runnable() {
      def run() {
        val rddBeginTime = System.currentTimeMillis()
        val jedis = redisPool.getResource
        try {
          jedis.hset("total_figure", "bytes_sum", rddUtil.getMsgBytesCount(sc, hbaseData).toString())
        } catch { 
          case ex: Exception => {
              ex.printStackTrace()
          }
        } finally {
            redisPool.returnResource(jedis)
        }
        val rddEndTime = System.currentTimeMillis()
        println("rdd batch处理时间："+(rddEndTime - rddBeginTime)/1000d+"s")
      }
    })
    
    println("Finished QueryTotalMsgInfo!!!!!!!!!")
  }
}