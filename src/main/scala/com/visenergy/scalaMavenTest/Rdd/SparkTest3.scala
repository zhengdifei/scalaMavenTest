package com.visenergy.scalaMavenTest.Rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//spark-submit --class com.visenergy.scalaMavenTest.SensorCount2 --executor-memory 2G scalaMavenTest-0.0.1-SNAPSHOT.jar spark://192.168.200.8:7077
object SparkTest3 {
  def main(args: Array[String]): Unit = {
    
	val conf = new SparkConf().setAppName("sparkTest").setMaster("local")
	val sc = new SparkContext(conf)
    
	val rdd1 = sc.parallelize(Array(1,2,3))
	val rdd2 = sc.parallelize(Array(3,4,5))
	//union
	val rdd3 = rdd1.union(rdd2)
	rdd3.foreach(println)
	
	//intersection
	val rdd4 = rdd1.intersection(rdd2)
	rdd4.foreach(println)
	
	//subtract
	val rdd5 = rdd1.subtract(rdd4)
	rdd5.foreach(println)
	
	//cartesian
	val rdd6 = rdd1.cartesian(rdd2)
	rdd6.foreach(println)
	
  }

}