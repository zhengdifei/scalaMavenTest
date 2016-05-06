package com.visenergy.scalaMavenTest.SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
/**
 * SparkStreaming测试实验
 * 
 * D:\project_room\nodeWorkspace\nodeTest\baseTest\FileForSpark.js
 * 监控文件系统
 * 
 */
object SparkSteamingTest5 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkStreamingTest").setMaster("local[2]");
    
    val scc = new StreamingContext(conf,Seconds(3));
    
    scc.checkpoint("test/point")
    val lines = scc.socketTextStream("localhost", 9999)
    
    val addFunc = (currValues:Seq[Int],prevValueState:Option[Int]) =>{
      //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
      val currentCount = currValues.sum;
      // 已累加的值
      val previousCount = prevValueState.getOrElse(0);
      // 返回累加后的结果，是一个Option[Int]类型
      Some(currentCount + previousCount)
    }
    
    def addFunc1(values : Seq[Int],state:Option[Int]) = {
      println(values)
      println(state)
      Some(state.getOrElse(0)+values.size)
    }
    //flatMap
    val testLines = lines.flatMap(x =>x.split(" "))
    
    val pairs = testLines.map(x => (x,1))
    //updateStateByKey
    //val totalWordCounts = pairs.updateStateByKey[Int](addFunc)
    
    val totalWordCounts = pairs.updateStateByKey(addFunc1 _)
    
    totalWordCounts.print
    
    scc.start
    scc.awaitTermination
  }

}