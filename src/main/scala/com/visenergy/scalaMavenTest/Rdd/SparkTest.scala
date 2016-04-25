package com.visenergy.scalaMavenTest.Rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
//spark-submit --class com.visenergy.scalaMavenTest.SensorCount2 --executor-memory 2G scalaMavenTest-0.0.1-SNAPSHOT.jar spark://192.168.200.8:7077
object SparkTest {
  def main(args: Array[String]): Unit = {
    
	val conf = new SparkConf().setAppName("sparkTest").setMaster("spark://192.168.100.101:7077")
	val sc = new SparkContext(conf)
    
	val rdd1 = sc.makeRDD(List(("A",4),("A",7),("C",3),("A",4),("B",5)))
	
	val rdd2 = rdd1.groupByKey().map(t => (t._1,t._2 .sum/t._2 .size,t._2.sum))
	
	
	rdd2.foreach(println)
  }

}