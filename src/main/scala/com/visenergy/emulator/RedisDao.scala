package com.visenergy.emulator

import redis.clients.jedis.JedisPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig

object RedisDao extends Serializable {

  @transient private var pool:JedisPool = null
    	      
  def makePool(redisHost:String="localhost",redisPort:Int=6379,redisTimeout:Int=30000,
      maxTotal:Int=10,maxIdle:Int=10,minIdle:Int=1):Unit = {
    makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle, true, false, 10000)
  }
  
  def makePool(redisHost:String,redisPort:Int,redisTimeout:Int,
      maxTotal:Int,maxIdle:Int,minIdle:Int,
      testOnBorrow:Boolean,testOnReturn:Boolean,maxWaitMillis:Long):Unit = {
    if(pool == null){
      val poolConfig = new GenericObjectPoolConfig()
      
      poolConfig.setMaxIdle(maxIdle)

      poolConfig.setMinIdle(minIdle)

      poolConfig.setTestOnBorrow(testOnBorrow)

      poolConfig.setTestOnReturn(testOnReturn)

      poolConfig.setMaxWaitMillis(maxWaitMillis)

      pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)

    }
  }
  
  def getPool : JedisPool = {
    pool
  }
  
  def main(args: Array[String]): Unit = {
    RedisDao.makePool("node13")
    val redisClient = RedisDao.getPool.getResource()
    RedisDao.getPool.returnResource(redisClient)
  }

}