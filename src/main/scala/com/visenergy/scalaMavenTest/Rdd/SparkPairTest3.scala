package com.visenergy.scalaMavenTest.Rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
//spark-submit --class com.visenergy.scalaMavenTest.SensorCount2 --executor-memory 2G scalaMavenTest-0.0.1-SNAPSHOT.jar spark://192.168.200.8:7077
object SparkPairTest3 {
  def main(args: Array[String]): Unit = {
    
	val conf = new SparkConf().setAppName("sparkTest").setMaster("local")
	val sc = new SparkContext(conf)
    
	val rdd1 = sc.parallelize(List((1,2),(3,4),(3,6)))
	
	//countByKey
	rdd1.countByKey().foreach(println)
	
	//collectAsMap
	println(rdd1.collectAsMap().toString)
	
	//lookup
	rdd1.lookup(3).foreach(println)
  }

}