package com.visenergy.scalaMavenTest.Rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//spark-submit --class com.visenergy.scalaMavenTest.SensorCount2 --executor-memory 2G scalaMavenTest-0.0.1-SNAPSHOT.jar spark://192.168.200.8:7077
object SparkTest4 {
  def main(args: Array[String]): Unit = {
    
	val conf = new SparkConf().setAppName("sparkTest").setMaster("local")
	val sc = new SparkContext(conf)
    
	val rdd1 = sc.parallelize(Array(1,2,3,3))
	//collect
	rdd1.collect.foreach(println)
	//count
	println(rdd1.count)
	//take
	rdd1.take(2).foreach(println)
	//top
	rdd1.top(2).foreach(println)
	//takeOrdered?
	rdd1.takeOrdered(2).foreach(println)
	//takeSample?
	rdd1.takeSample(false, 1).foreach(println)
	//reduce
	println(rdd1.reduce((x,y) => x + y).toString)
	println(rdd1.reduce(_+_).toString)
	//fold?
	println(rdd1.fold(0)(_+_))
	println(rdd1.fold(1)(_+_))
	println(rdd1.fold(2)(_+_))
	//aggregate?
	println(rdd1.aggregate((0,0))((x,y)=>(x._1 + y,x._2 +1), (x,y)=>(x._1 +y._1 ,x._2 +y._2 )).toString)
	
  }

}