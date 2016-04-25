package com.visenergy.scalaMavenTest.Rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
//spark-submit --class com.visenergy.scalaMavenTest.SensorCount2 --executor-memory 2G scalaMavenTest-0.0.1-SNAPSHOT.jar spark://192.168.200.8:7077
object SparkPairTest2 {
  def main(args: Array[String]): Unit = {
    
	val conf = new SparkConf().setAppName("sparkTest").setMaster("local")
	val sc = new SparkContext(conf)
    
	val rdd1 = sc.parallelize(List((1,2),(3,4),(3,6)))
	val rdd2 = sc.parallelize(List((3,9),(3,10)))
	
	//subtractByKey
	rdd1.subtractByKey(rdd2).foreach(println)
	
	//join
	rdd1.join(rdd2).foreach(println)
	
	//rightOutJoin
	rdd1.rightOuterJoin(rdd2).foreach(println)
	
	//leftOutJoin
	rdd1.leftOuterJoin(rdd2).foreach(println)
	
	//cogroup
	rdd1.cogroup(rdd2).foreach(println)
	//combineByKey
	val rdd3 = rdd1.combineByKey((v)=>(v,1), (acc:(Int,Int),v)=>(acc._1+v,acc._2+1), (acc1:(Int,Int),acc2:(Int,Int))=>(acc1._1+acc2._1,acc1._2+acc2._2))
	val rdd4 =rdd3.map{case(key,value) => (key,value._1 /value._2 .toFloat)}
	rdd4.collectAsMap().map(println(_))
  }

}