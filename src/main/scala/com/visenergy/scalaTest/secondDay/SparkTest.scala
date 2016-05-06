package com.visenergy.scalaTest.secondDay

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkTest {

  def main(args: Array[String]): Unit = {
    
	val conf = new SparkConf().setAppName("sparkTest").setMaster("local")
	val sc = new SparkContext(conf)
    
	
	val rdd1 = sc.parallelize(List(("A",4),("A",7),("C",3),("A",4),("B",5)))
	
	val rdd2 = rdd1.groupByKey().map(t => (t._1,t._2 .sum/t._2 .size,t._2.sum))
	
	rdd2.foreach(println)
  }

}