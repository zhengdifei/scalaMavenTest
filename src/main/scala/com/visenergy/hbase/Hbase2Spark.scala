package com.visenergy.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 通过spark读取hbase里面的数据
 */
object Hbase2Spark {
  
  def main(args: Array[String]): Unit = {
	val conf = HBaseConfiguration.create()
	conf.set("hbase.zookeeper.quorum","localhost")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  
  //设置查询的表名
  conf.set(TableInputFormat.INPUT_TABLE, "user")

  val sparkConf = new SparkConf().setAppName("hbaseTest3").setMaster("local")
  val sc = new SparkContext(sparkConf)
	val userRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])
	
	val count = userRDD.count
	println("user table have rows:" + count)
	userRDD.cache()
	
	//遍历输出
	userRDD.foreach{ case (_,result) => {
			val key = Bytes.toInt(result.getRow())
			val name = Bytes.toString(result.getValue("basic".getBytes(),"name".getBytes()))
			var age = 0;
			if(result.getValue("basic".getBytes(), "age".getBytes()) != null ){
			  age = Bytes.toInt(result.getValue("basic".getBytes(), "age".getBytes()))
			}
			println("Row key:" + key + " Name:" + name + " Age:"+ age)
		}
	}
  }

}