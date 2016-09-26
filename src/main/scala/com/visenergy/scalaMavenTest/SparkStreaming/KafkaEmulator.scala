package com.visenergy.scalaMavenTest.SparkStreaming

import java.util.HashMap
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.storage._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import sun.misc.BASE64Decoder
import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream
import java.util.zip.ZipInputStream

object KafkaEmulator {
    def main(args: Array[String]) {
          val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
          val ssc = new StreamingContext(sparkConf, Seconds(2))
          
          val lines = KafkaUtils.createStream(ssc, "localhost:2181", "myGroup", Map("sensorData" -> 1), StorageLevel.MEMORY_AND_DISK_SER_2).map(_._2)
       //  lines.print()
          def strSubstring(s:String):String={
            //截取MD5码
		    var tempStr=s.substring(0,s.length()-24)
		    //解压缩
		    val compressed:Array[Byte] = new BASE64Decoder().decodeBuffer(tempStr)
		    var out:ByteArrayOutputStream = new ByteArrayOutputStream()
		    val in:ByteArrayInputStream = new ByteArrayInputStream(compressed)
		    val zin:ZipInputStream  = new ZipInputStream(in);
		   // zin.getNextEntry()
		    val buffer:Array[Byte] =new Array[Byte](1024);
			var offset = -1
			offset=zin.read()
			println(offset)
			while (offset != -1) {
				out.write(buffer, 0, offset)
				offset=zin.read(buffer)
			}
			val decompressed=out.toString()
			print(decompressed)
		    decompressed
		  }
          val words = lines.flatMap(strSubstring)
          words.print()
          ssc.start()
          ssc.awaitTermination()	
    }
}