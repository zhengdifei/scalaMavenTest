package com.visenergy.scalaMavenTest.Rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//spark-submit --class com.visenergy.scalaMavenTest.SensorCount2 --executor-memory 2G scalaMavenTest-0.0.1-SNAPSHOT.jar spark://192.168.200.8:7077
object SparkTest2 {
  def main(args: Array[String]): Unit = {
    
	val conf = new SparkConf().setAppName("sparkTest").setMaster("local")
	val sc = new SparkContext(conf)
    
	val rdd1 = sc.parallelize(Array(1,2,3,3))
	//map
	val rdd2 = rdd1.map(x => x+1)
	rdd2.foreach(println)
	
	//flatMap
	val rdd3 = rdd1.flatMap(x => x.to(4))
	rdd3.foreach(println)
	
	//filter
	val rdd4 = rdd1.filter(x => x != 1)
	rdd4.foreach(println)
	
	//distinct
	val rdd5 = rdd1.distinct
	rdd5.foreach(println)
	
	//sample
	val rdd6 = rdd3.sample(false, 0.1)
	rdd6.foreach(println)
  }

}