package com.visenergy.scalaMavenTest.Rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.util.control.Breaks._
/**
 * Created by xuyao on 15-7-24.
 * 求中位数，数据是分布式存储的
 * 将整体的数据分为K个桶，统计每个桶内的数据量，然后统计整个数据量
 * 根据桶的数量和总的数据量，可以判断数据落在哪个桶里，以及中位数的偏移量
 * 取出这个中位数
 * 
 * 这种方式适合数据匀速变化的情况，如果数据波动太大，或者太小，就无法对一组数据进行均分
 */
object SparkTest5 {
  def main(args: Array[String]): Unit = {
    
	val conf = new SparkConf().setAppName("sparkTest").setMaster("local")
	val sc = new SparkContext(conf)
    
	val rdd1 = sc.makeRDD(List(1,2,3,4,5 ,6 ,8 ,9, 11 ,12 ,13 ,15, 18, 20 ,22 ,23 ,25 ,27 ,29,32))
	//将数据分为5组，当然我这里的数据少
	val mappeddata = rdd1.map(x =>(x/5,x)).sortByKey()
	//将数据分为5组，当然我这里的数据少
	val p_count = rdd1.map(x => (x/5,1)).reduceByKey(_+_).sortByKey()
	p_count.foreach(println)
	//p_count是一个RDD，不能进行Map集合操作，所以要通过collectAsMap方法将其转换成scala的集合
	val scala_p_count = p_count.collectAsMap()
	//根据key值得到value值
	println(scala_p_count(0))
	//sum_count是统计总的个数，不能用count(),因为会得到多少个map对。
	val sum_count = p_count.map(x=>x._2).sum.toInt
	println(sum_count)
	
	var temp = 0//中值所在的区间累加的个数
	var temp2 = 0//中值所在区间的前面所有的区间累加的个数
	var index = 0//中值的区间
	var mid = 0
	
	if(sum_count%2 != 0){
	  mid = sum_count/2 + 1//中值在整个数据的偏移量
	}else{
	  mid = sum_count/2
	}
	
	val pcount = p_count.count
	breakable {
	  for(i <- 0 to pcount.toInt -1){
	  temp = temp + scala_p_count(i)
	  temp2 = temp - scala_p_count(i)
	  if(temp >= mid){
	    index = i
	    break;
	  }
	}
	}
	
	println(mid+" "+index+" "+temp+" "+temp2)
	//中位数在桶中的偏移量
	val offset = mid - temp2
	//takeOrdered它默认可以将key从小到大排序后，获取rdd中的前n个元素
	val result = mappeddata.filter(x => x._1 == index).takeOrdered(offset)
	println(result(offset-1)._2 )
	
	sc.stop()
	
  }

}