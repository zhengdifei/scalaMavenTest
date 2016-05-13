package com.visenergy.tdj.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object HBase2Spark {
  def main(args:Array[String]) {
    val sparkConf = new SparkConf().setAppName("HBase2Spark").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    getHBaseRdd(sc, 1L, 3L).foreach(println)
  }
  
  def getHBaseRdd(sc:SparkContext, startTime:Long, endTime:Long) : RDD[String] = {
    val conf = HBaseConfiguration.create() 
    conf.set("hbase.zookeeper.quorum",HBaseConfig.HBASE_ZK_IP)
    conf.set("hbase.zookeeper.property.clientPort", HBaseConfig.HBASE_ZK_PORT)
    conf.set(TableInputFormat.INPUT_TABLE, HBaseConfig.HBASE_TABLE)
    conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, HBaseConfig.COLUMN_FAMILY)
    conf.set(TableInputFormat.SCAN_ROW_START, startTime.toString())
    conf.set(TableInputFormat.SCAN_ROW_STOP, endTime.toString())
    
  	val hbaseRdd = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], 
  	    classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])
    
    hbaseRdd.map(line => Bytes.toString((line._2).value())) 
  }
}