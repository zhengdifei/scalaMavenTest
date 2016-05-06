package com.visenergy.scalaMavenTest.Rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
/**
 * 
 */
object SparkTest8 {
  def main(args: Array[String]): Unit = {
    
	val conf = new SparkConf().setAppName("sparkTest").setMaster("local")
	val sc = new SparkContext(conf)
    
	val rdd1 = sc.makeRDD(1 to 10,3)
	//aggregate
	val rdd2 = rdd1.aggregate(1)(
	    {(x,y) => x + y},
	    {(a,b) => a + b}
	)
	println(rdd2)
	
	//fold
	val rdd3 = rdd1.fold(1)(
		(x,y) => x + y
	)
	println(rdd3)
	
	//aggregate
	val rdd4 = rdd1.aggregate(1)(
	    {(x,y) => x + y},
	    {(a,b) => a * b}
	)
	println(rdd4)
  }

}