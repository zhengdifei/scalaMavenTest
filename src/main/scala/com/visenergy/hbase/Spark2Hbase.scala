package com.visenergy.hbase114

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
/**
 *	向spark中写入数据
 */
object Spark2Hbase {
  
  def main(args: Array[String]): Unit = {
	val conf = HBaseConfiguration.create()
	conf.set("hbase.zookeeper.quorum","localhost")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  
  val jobConf = new JobConf(conf,this.getClass())
	jobConf.setOutputFormat(classOf[TableOutputFormat])
	jobConf.set(TableOutputFormat.OUTPUT_TABLE,"user")

	def convert(triple:(Int,String,Int)) = {
	  val p = new Put(Bytes.toBytes(triple._1 ))
	  p.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("name"), Bytes.toBytes(triple._2))
  	p.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("age"), Bytes.toBytes(triple._3))
  	  (new ImmutableBytesWritable,p)
	}
	
	val sparkConf = new SparkConf().setAppName("hbaseTest").setMaster("local")
	val sc = new SparkContext(sparkConf)
	val rawData = List((4,"lilei1",21),(5,"hanmei1",24),(6,"zhengfei1",19))
	val localData = sc.parallelize(rawData).map(convert)
	
	localData.saveAsHadoopDataset(jobConf)
  }

}