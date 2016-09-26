package com.visenergy.scalaMavenTest.Rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
//spark-submit --class com.visenergy.scalaMavenTest.SensorCount2 --executor-memory 2G scalaMavenTest-0.0.1-SNAPSHOT.jar spark://192.168.200.8:7077
object SparkPairTest {
  def main(args: Array[String]): Unit = {
    
	val conf = new SparkConf().setAppName("sparkTest").setMaster("local")
	val sc = new SparkContext(conf)
    
	val rdd1 = sc.parallelize(List((1,2),(3,4),(3,6)))
	
	//reduceByKey	
	rdd1.reduceByKey((x,y)=>x+y).foreach(println)
	rdd1.reduceByKey(_+_).foreach(println)
	
	//groupByKey
	rdd1.groupByKey().foreach(println)
	
	//mapValues
	rdd1.mapValues(x => x + 1).foreach(println)
	
	//flatMapValues
	rdd1.flatMapValues(x => (x to 5)).foreach(println)
	
	//keys
	rdd1.keys.foreach(println)
	
	//values
	rdd1.values.foreach(println)
	
	//sortBykey
	rdd1.sortByKey().foreach(println)
  }

}