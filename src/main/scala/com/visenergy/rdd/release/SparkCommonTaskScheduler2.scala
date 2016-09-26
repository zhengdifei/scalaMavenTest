package com.visenergy.rdd.release

import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.ScheduledExecutorService
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.concurrent.TimeUnit
import redis.clients.jedis.Jedis
import org.apache.spark.serializer.KryoSerializer
import redis.clients.jedis.JedisPool;  
import redis.clients.jedis.JedisPoolConfig; 

object SparkCommonTaskScheduler2 {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("SparkCommonTaskScheduler2")
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    
    var config:JedisPoolConfig = new JedisPoolConfig()
    config.setMaxTotal(500);
    config.setMaxWaitMillis(1000 * 100);
    config.setTestOnBorrow(true);
    val redisPool = new JedisPool(config, BaseConfig.REDIS_IP, BaseConfig.REDIS_PORT); 
    
    val rddOp:RddOpUtil = new RddOpUtil()
    
    val scheduler:ScheduledExecutorService  = Executors.newScheduledThreadPool(BaseConfig.SCHEDULER_THREAD_POOL_SIZE)
    scheduler.scheduleAtFixedRate(new Runnable() {
      def run() {
        var now = System.currentTimeMillis()
        val hbaseData = HBase2Spark.getHBaseRdd(sc, 0L, now)
          
        val es:ExecutorService = Executors.newCachedThreadPool()
        
        // sales_count 
        es.execute(new Runnable() {
          def run() {
            val jedis = redisPool.getResource
            try {
              jedis.hset("total_figure", "sales_count", rddOp.getMsgCount(hbaseData).toString())
            } catch {
              case ex: Exception => {
                  ex.printStackTrace()
              }
            } finally {
                redisPool.returnResource(jedis)
            }
          }
        })
        
        // sales_amount
        es.execute(new Runnable() {
          def run() {
            val jedis = redisPool.getResource
            try {
              jedis.hset("total_figure", "sales_amount", rddOp.getSumByKey(hbaseData, BaseConfig.SALES_AMOUNT_PROPERTY).toString())
            } catch {
              case ex: Exception => {
                  ex.printStackTrace()
              }
            } finally {
                redisPool.returnResource(jedis)
            }
          }
        })
        
        // mid_price
        es.execute(new Runnable() {
          def run() {
            val jedis = redisPool.getResource
            try {
              jedis.hset("total_figure", "mid_price", rddOp.getSectionNumNew(hbaseData, BaseConfig.SALES_AMOUNT_PROPERTY, 0.5).toString())
            } catch {
              case ex: Exception => {
                  ex.printStackTrace()
              }
            } finally {
                redisPool.returnResource(jedis)
            }
          }
        })
        
        // avg_price
        es.execute(new Runnable() {
          def run() {
            val jedis = redisPool.getResource
            try {
              jedis.hset("total_figure", "avg_price", rddOp.getAvgByKey(hbaseData, BaseConfig.SALES_AMOUNT_PROPERTY).toString())
            } catch {
              case ex: Exception => {
                  ex.printStackTrace()
              }
            } finally {
                redisPool.returnResource(jedis)
            }
          }
        })
        
        // outlet_count
        es.execute(new Runnable() {
          def run() {
            val jedis = redisPool.getResource
            try {
              val result = rddOp.getMsgCountDistribution(hbaseData, BaseConfig.ID_PROPERTY)
              for(tuple <- result) {
                jedis.hset("outlet_count", tuple._1, tuple._2.toString())
              }
            } catch {
              case ex: Exception => {
                  ex.printStackTrace()
              }
            } finally {
                redisPool.returnResource(jedis)
            }
          }
        })
        
        // outlet_amount
        es.execute(new Runnable() {
          def run() {
            val jedis = redisPool.getResource
            try {
              val result = rddOp.getMsgSumDistribution(hbaseData, BaseConfig.ID_PROPERTY, BaseConfig.SALES_AMOUNT_PROPERTY)
              for(tuple <- result) {
                jedis.hset("outlet_amount", tuple._1, tuple._2.toString())
              }
            } catch {
              case ex: Exception => {
                  ex.printStackTrace()
              }
            } finally {
                redisPool.returnResource(jedis)
            }
          }
        })
      }
    }, 0L, BaseConfig.SCHEDULER_INTERVAL, TimeUnit.SECONDS) 
  }
}