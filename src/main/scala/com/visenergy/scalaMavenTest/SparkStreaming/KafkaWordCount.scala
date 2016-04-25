package com.visenergy.scalaMavenTest.SparkStreaming

import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.storage._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object KafkaWordCount {
    def main(args: Array[String]) {
          val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
          val ssc = new StreamingContext(sparkConf, Seconds(2))
          
          val lines = KafkaUtils.createStream(ssc, "localhost:2181", "myGroup", Map("twoPartitionTopic" -> 2), StorageLevel.MEMORY_AND_DISK_SER_2).map(_._2)
          val words = lines.flatMap(_.split(" "))
          val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)
          wordCounts.print()
          
//          val lines = KafkaUtils.createStream(ssc, "localhost:2181", "myGroup", Map("myTopic" -> 1), StorageLevel.MEMORY_AND_DISK_SER_2)
//          wordCounts.print()
//          lines.foreachRDD(line => line.foreach(println))
          
          ssc.start()
          ssc.awaitTermination()	
    }
}