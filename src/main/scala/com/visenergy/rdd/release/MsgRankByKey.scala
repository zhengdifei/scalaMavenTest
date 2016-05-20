package com.visenergy.rdd.release

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import redis.clients.jedis.Jedis

object MsgRankByKey {
  def main(args:Array[String]) {
    if (args.length <= 0) {
      println("please input params")
      return;
    }
    
    val targetKey = args(0)
    val conf = new SparkConf().setAppName("MsgRankByKey")
    val sc = new SparkContext(conf)
    val rddOp:RddOpUtil = new RddOpUtil()
    
    var hbaseRdd = HBase2Spark.getHBaseRdd(sc, 0L, System.currentTimeMillis())
    
    val rankResult = rddOp.getMsgRank(hbaseRdd, targetKey).take(BaseConfig.RANK_BY_KEY_FIRST_NUM)
    val jedis = new Jedis(BaseConfig.REDIS_IP, BaseConfig.REDIS_PORT)
    val rankKey = "rank_" + targetKey
    for(tuple <- rankResult) {
      jedis.hset(rankKey, tuple._1.toString(), tuple._2.toString())
      jedis.expire(rankKey, BaseConfig.REDIS_KEY_TIMEOUT)
    }
	  
	  Thread.sleep(300000)
	  
  }
  
}