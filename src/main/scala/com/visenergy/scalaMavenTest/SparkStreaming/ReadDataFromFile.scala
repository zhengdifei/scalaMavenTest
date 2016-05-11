package com.visenergy.scalaMavenTest.SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.NullWritable
import com.esotericsoftware.kryo.io.Input
import org.apache.hadoop.io.BytesWritable
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SQLContext
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature

case class Emulator(SID:String,SNAME:String,time:Long)

object ReadDataFromFile {
  /*
   * 利用kryo进行序列化对象并进行读写
   */
  def saveAsObjectFileKryo[T:ClassTag](rdd:RDD[T],path:String){
    val kryoSerializer = new KryoSerializer(rdd.context.getConf)
    
    rdd.mapPartitions(iter=>iter.grouped(10).map(_.toArray)).map(splitArray => {
      //initializes kyro and calls your registrator class
      val kryo = kryoSerializer.newKryo;
      //convert data to bytes
      import java.io._
      val bao = new ByteArrayOutputStream()
      val output = kryoSerializer.newKryoOutput()
      output.setOutputStream(bao)
      kryo.writeClassAndObject(output, splitArray)
      output.close()
      
      // We are ignoring key field of sequence file
      val bytesWritable = new BytesWritable(bao.toByteArray())
      (NullWritable.get(),bytesWritable)
    }).saveAsSequenceFile(path)
  }
  
  def objectFileKryo[T](sc:SparkContext,path:String,minPartitions:Int = 1)
    (implicit ct:ClassTag[T]) = {
      val kryoSerializer = new KryoSerializer(sc.getConf)
      val rdd1 = sc.sequenceFile(path, classOf[NullWritable], classOf[BytesWritable],minPartitions)
      rdd1.flatMap(x => {
        val kryo = kryoSerializer.newKryo
        val input = new Input()
        input.setBuffer(x._2 .getBytes())
        val data = kryo.readClassAndObject(input)
        val dataObject = data.asInstanceOf[Array[T]]
        dataObject
      })
  }
    
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("readFromFile").setMaster("local")
    //conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    //文本文件
    //val rdd1 = sc.textFile("test/obj/zdf*")
    //seq[Null,Bytes]文件
    val rdd1 = sc.sequenceFile("test/obj/zdf",classOf[NullWritable],classOf[BytesWritable])
    //java默认序列化对象，解析sequenceFile文件的方式
    val rdd2  = rdd1.map(x =>x._2).flatMap(x=>{
    	import java.io._
		val bis = new ByteArrayInputStream(x.get())
		val ois = new ObjectInputStream(bis)
    	val arr:Array[String] = ois.readObject().asInstanceOf[Array[String]]
    	arr
    })
    
    //seq[Null,Text]
//    val rdd1 = sc.sequenceFile("test/kryo/zdf",classOf[NullWritable],classOf[Text])
//    //java默认序列化对象，解析sequenceFile文件的方式
//    val rdd2  = rdd1.map(x =>x._2)
    //保存kryo序列化对象
    //saveAsObjectFileKryo(rdd2,"test/kryo/zdf")
    //rdd2.foreach(println)
    //更简单读取默认存储的sequenceFile文件
//    val rdd3 = sc.objectFile("test/kryo/zdf")
//    rdd3.foreach(println)
    //读取kryo序列化对象
//    val rdd4 = objectFileKryo(sc,"test/kryo/zdf")
//    rdd4.foreach(println)
    
//    //parquet文件的读取
//    val ssc = new SQLContext(sc)
//    val df1 = ssc.parquetFile("test/table/zdf")
//    df1.foreach(println)
    //json文件读取,直接转化成对象或者Map失败，字符串成功
    val mapper = new ObjectMapper()
    mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true)
    val jsonRdd = sc.textFile("test/json/zdf2").map(r =>{
      try{
        Some(mapper.readValue(r,classOf[String]))
      }catch{
        case e:Exception =>None
      }
    })
    println(jsonRdd.count)
    jsonRdd.foreach(println)
  }

}