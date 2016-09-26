package com.visenergy.rdd.release

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.immutable.List
import redis.clients.jedis.Jedis

object AndOpByKey {
  def main(args:Array[String]) {
    
    if (args.length <= 0) {
      println("please input params")
      println("Usage: key,value,key2,value2.......")
      return;
    }
    
    val conf = new SparkConf().setAppName("AndOpByKey")
    val sc = new SparkContext(conf)
    val rddOp:RddOpUtil = new RddOpUtil()
    
    var hbaseRdd = HBase2Spark.getHBaseRdd(sc, 0L, System.currentTimeMillis())
    var pairList = List[Tuple2[String, String]]()
    
    for(param <- 0 to args.length/2 - 1) {
      pairList ::= (args(param*2), args(param*2 + 1))
    }
    
    val andResult = rddOp.getAndOpMsgCount(hbaseRdd, pairList)
    var targetKey = ""
    pairList.reverse.foreach(e => {
      targetKey = targetKey + e._1 + "_"
    })
    
    targetKey = targetKey + "andOp"
    
    val jedis = new Jedis(BaseConfig.REDIS_IP, BaseConfig.REDIS_PORT)
    jedis.set(targetKey, andResult.toString())
    jedis.expire(targetKey, BaseConfig.REDIS_KEY_TIMEOUT)
	  
	  Thread.sleep(300000)
	  
  }
}