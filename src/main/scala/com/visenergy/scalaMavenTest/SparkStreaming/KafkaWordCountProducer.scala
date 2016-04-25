package com.visenergy.scalaMavenTest.SparkStreaming

import java.util.HashMap
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object KafkaWordCountProducer {
    def main(args: Array[String]) {
   
      val props = new HashMap[String, Object]()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094")
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer")
  
      val producer = new KafkaProducer[String, String](props)
  
      // Send some messages
      while(true) {
          (1 to 5).foreach { messageNum =>
            val str = (1 to 6).map(x => scala.util.Random.nextInt(10).toString)
              .mkString(" ")
    
            val message = new ProducerRecord[String, String]("twoPartitionTopic", null, str)
            producer.send(message)
          }
  
          Thread.sleep(1000)
      }
    }          
}