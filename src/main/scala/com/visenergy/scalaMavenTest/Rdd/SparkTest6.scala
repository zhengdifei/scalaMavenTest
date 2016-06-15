package com.visenergy.scalaMavenTest.Rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.util.control.Breaks._
/**
 * Created by xuyao on 15-7-24.
 * 累加器+rdd结构
 */
object SparkTest6 {
  def main(args: Array[String]): Unit = {
    
	val conf = new SparkConf().setAppName("sparkTest").setMaster("local")
	val sc = new SparkContext(conf)
    
	val rdd1 = sc.makeRDD(List(1,2,3,4,5 ,6 ,8 ,9, 11 ,12 ,13 ,15, 18, 20 ,22 ,23 ,25 ,27 ,29,32))
	//accumulator
	var cnt = sc.accumulator(0)
	//foreach
	rdd1.foreach(x => cnt +=x)
	
	println(cnt.value)
	
	val rdd2 = sc.makeRDD(List(1,2,3,4,5 ,6 ,8 ,9, 11 ,12 ,13 ,15, 18, 20 ,22 ,23 ,25 ,27 ,29,32),4)
	var cnt2 = sc.accumulator(0)
	//foreachPartition
	rdd2.foreachPartition(x => cnt2+= x.size)
	
	println(cnt2.value)
	//foreachPartition
	rdd2.foreachPartition((x:Iterator[Int]) => {
	  while(x.hasNext){
	    println(x.next)
	  }
	})

	val rdd3 = sc.makeRDD(List(1,4,20,12,22,5 ,9, 11 ,13 ,6,32 ,55,8 ,15, 18, 23 ,2,3,25 ,27 ,29),4)
	//sortBy
	rdd3.sortBy(x=>x,false).foreach(println)
	//partitions
	rdd3.partitions.foreach{partition =>{
			println("index:" + partition.index + "  hasCode:" + partition.hashCode())
		}
	}
	
	rdd3.dependencies.foreach{ dep =>
	  println("dependency type:" + dep.getClass)
	  println("dependency RDD:" + dep.rdd)
      println("dependency partitions:" + dep.rdd.partitions)
      println("dependency partitions size:" + dep.rdd.partitions.length)
	}
  }

}