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
object SparkSteamingTest3 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkStreamingTest").setMaster("local[2]");
    
    val scc = new StreamingContext(conf,Seconds(20));
    
    scc.checkpoint("test/point")
    //textFileStream
    val lines = scc.textFileStream("test/file")
    
    val testLines = lines.flatMap(_.split(" "))
    
    testLines.print;
    
    scc.start
    scc.awaitTermination
  }
}