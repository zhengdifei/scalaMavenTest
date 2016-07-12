package com.visenergy.scalaMavenTest.Rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
/**
 * 通过mapPartitionsWithInde求中位数
 */
object SparkTest7 {
  def main(args: Array[String]): Unit = {
    
	val conf = new SparkConf().setAppName("sparkTest").setMaster("local")
	val sc = new SparkContext(conf)
    
	val rdd1 = sc.makeRDD(List(1,4,20,12,22,5 ,9, 11 ,13 ,6,32 ,55,8 ,15, 18, 23 ,2,3,25 ,27 ,29),5)
	
	//mapPartitionsWithIndex
	val rdd2 = rdd1.sortBy(x=>x,true).mapPartitionsWithIndex{
	  (partIdx,iter) => {
	    var part_map = scala.collection.mutable.Map[String,List[Int]]()
	    while(iter.hasNext){
	      var part_name = "part_" + partIdx
	      var elem = iter.next
	      if(part_map.contains(part_name)){
	        var elems = part_map(part_name)
	        elems ::= elem
	        part_map(part_name) = elems
	      }else{
	        part_map(part_name) = List[Int]{elem}
	      }
	    }
	    part_map.iterator
	  }
	}
	
	//rdd2.foreach(println)
	
	var count = rdd1.sum
	println(count)
	
	val rdd3 = rdd1.sortBy(x=>x,true).mapPartitionsWithIndex{ 
	  (partIdx,iter) =>{
		 var result = List[Int]()
		 if(partIdx == 2){
			while(iter.hasNext){
			  result ::=(iter.next)
			}
		 }
		 result.iterator
	  }
	}
	println(rdd3.count)
	rdd3.foreach(println)
	
	//mapPartitions
	val rdd4 = rdd1.sortBy(x=>x,true).mapPartitions(x=>{
	  var result = List[Int]()
	  var i = 0
	  while(x.hasNext){
	    i += x.next
	  }
	  result.::(i).iterator
	})
	
	//println(rdd4.count)
	//rdd4.foreach(println)
  }

}