package com.visenergy.rdd.release

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import redis.clients.jedis.Jedis

object QueryDataDistributionByKey {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("QueryDataDistributionByKey")
    val sc = new SparkContext(conf)
    val rddOp:RddOpUtil = new RddOpUtil()
    
    val now = System.currentTimeMillis()
    var hbaseData = HBase2Spark.getHBaseRdd(sc, 0L, now)
    val timeSpan = 365L * 24L * 60L * 60L * 1000L;
    
    val result = rddOp.getCountDistributionByTimeSpan(hbaseData, BaseConfig.DATA_DISTRIBUTION_KEY, now-timeSpan, now, BaseConfig.DATA_DISTRIBUTION_NODES)
    val jedis = new Jedis(BaseConfig.REDIS_IP, BaseConfig.REDIS_PORT)
    for(tuple <- result) {
      jedis.hset("sales_count_by_month", tuple._1.toString(), tuple._2.toString())
    }
    
    Thread.sleep(300000)
  }
}