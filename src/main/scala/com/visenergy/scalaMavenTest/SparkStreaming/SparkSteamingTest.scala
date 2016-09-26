package com.visenergy.scalaMavenTest.SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.rdd.RDD
/**
 * SparkStreaming测试实验
 * nc -l -p ip port模拟httpServer
 * 
 * D:\project_room\nodeWorkspace\nodeTest\Tcp\server.js模拟数据
 * 基本的DStream操作
 */
object SparkSteamingTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkStreamingTest").setMaster("local[2]");
    
    val scc = new StreamingContext(conf,Seconds(3));
    
    val lines = scc.socketTextStream("localhost", 9999)
    
    //map
    //val testLines = lines.map(x => x + 1);
    
    //filter
    //val testLines = lines.filter(_.contains("a"))
    
    //flatMap
    //val testLines = lines.flatMap(x =>x.split(" "))
    
    //repartition
    //val testLines = lines.flatMap(x =>x.split(" ")).repartition(10);
    
    //reduceByKey
    //val testLines = lines.flatMap(x =>x.split(" ")).map(x=>(x,1)).reduceByKey(_+_);
    
    //groupByKey
    //val testLines = lines.flatMap(x =>x.split(" ")).map(x=>(x,1)).groupByKey();
    
    //join
    //val test1 = lines.map(x=>(x,1));
    //val test2 = lines.map(x=>(x,2));
    
    //val testLines = test1.join(test2);
    
    //union
    //val testLines = test1.union(test2);
    
    //transform
    def spaceSplit(rdd:RDD[String]):RDD[String]={
      rdd.flatMap(_.split(" "))
    }
    //transform
    //val testLines = lines.transform(rdd => spaceSplit(rdd));
    val testLines = lines.transform{rdd => spaceSplit(rdd)};
    
    testLines.print()
    //必须启动sparkStreaming，并等待作业完成
    scc.start
    scc.awaitTermination
  }

}