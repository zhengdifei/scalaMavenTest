package com.visenergy.scalaMavenTest.Rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
/**
 * 
 */
object SparkTest9 {
  def main(args: Array[String]): Unit = {
    
	val conf = new SparkConf().setAppName("sparkTest").setMaster("local")
	val sc = new SparkContext(conf)
    
	val rdd1 = sc.makeRDD(List(1,4,20,12,22,5 ,9, 11 ,13 ,6,32 ,55,8 ,15, 18, 23 ,2,3,25 ,27 ,29),5)
	//foreach是无序的
	//rdd1.foreach(print)
	//rdd1.foreach(print)
	
	//foreachPartition
	//rdd1.foreachPartition(x=>println(x.next))
	
	//mapPartition
	//rdd1.mapPartitions(x=>Array(x.next).iterator).foreach(println)
	
	//keyBy
	rdd1.keyBy(x=>x/4).foreach(println)
	
	//groupBy
	//rdd1.groupBy(_/4).foreach(println)
	
	//collectPartitions
	val rdd2 = rdd1.sortBy(x=>x).keyBy(x=>x/4).toJavaRDD
	val p_size = rdd2.partitions.size()
	val mid_part = p_size/2
	rdd2.collectPartitions(Array(mid_part)).foreach(println)
	
	val rdd3 = rdd1.sortBy(x=>x).toJavaRDD
	rdd3.collectPartitions(Array(mid_part)).foreach(println)
	
	var acc = sc.accumulator(0)
	val rdd4 = rdd1.sortBy(x=>x)
	val size = rdd1.count()
	val mid = size/2
	var mid_value = 0
	rdd4.foreach(x =>{
			acc += 1
			if(acc.value == mid){
				mid_value = x
				println(mid_value)
			}
		}
	)
	
	
  }

}