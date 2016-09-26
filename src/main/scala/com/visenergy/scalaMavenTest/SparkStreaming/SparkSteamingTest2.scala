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
 * nc -l -p ip port模拟httpServer
 * 
 * D:\project_room\nodeWorkspace\nodeTest\Tcp\server.js模拟数据
 * SparkStreaming的window操作
 */
object SparkSteamingTest2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkStreamingTest").setMaster("local[2]");
    
    val scc = new StreamingContext(conf,Seconds(3));
    
    scc.checkpoint("test/point")
    val lines = scc.socketTextStream("localhost", 9999)
    
    //flatMap
    val testLines = lines.flatMap(x =>x.split(" "))
    //window
    //val testWindows = testLines.window(Seconds(9),Seconds(3));
    //reduceByWindow
    val testWindows = testLines.map(x=>x.length()).reduceByWindow(
        {_+_},
        {_-_},
        Seconds(9),
        Seconds(3)
    )
    //reduceByKeyAndWindow
//    val testWindows = testLines.map(x=> (x,1)).reduceByKeyAndWindow(
//        {(x,y) => x + y},
//        {(x,y) => x - y},
//        Seconds(9),
//        Seconds(3)
//    )
    
    //countByWindow
    val testWindows1 = testLines.countByWindow(Seconds(9),Seconds(3));
    //countByValueAndWindow
    val testWindows2 = testLines.countByValueAndWindow(Seconds(9),Seconds(3));
    //sequence?
    //val testLineSeqFile = testWindows2.map{(x:String,y:Long)=>(new Text(x),new LongWritable(y))}
    //testWindows.print();
    //testWindows1.print();
    //testWindows2.print();
    //File
    //testWindows2.saveAsTextFiles("test/output", "txt");
    //SequenceFile
    //testLineSeqFile.saveAsHadoopFiles[SequenceFileOutputFormat[Text,LongWritable]]("test/output","txt");
    //必须启动sparkStreaming，并等待作业完成
    scc.start
    scc.awaitTermination
  }

}