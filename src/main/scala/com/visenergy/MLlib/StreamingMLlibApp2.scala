package com.visenergy.MLlib

import scala.util.Random
import java.net.ServerSocket
import java.io.PrintWriter
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import java.text.SimpleDateFormat
import java.util.Date

object StreamingMLlibApp2 {

  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext("local[2]","streaming app",Seconds(10))
    
    ssc.checkpoint("test/checkpoint1/")
    
    val stream = ssc.socketTextStream("localhost", 9999)
    
    
    stream.print
    
    val events = stream.map{ record =>
      val event = record.split(",")
      (event(0),event(1),event(2).toDouble)
    }
    
    val users = events.map{ case (user,product,price) => (user,(product,price))}
    
    val revenuePerUser = users.updateStateByKey(updateState)
    revenuePerUser.print
    
    ssc.start
    ssc.awaitTermination
  }
  
  def updateState(prices : Seq[(String,Double)],currentTotal : Option[(Int,Double)]) = {
    val currentRevenue = prices.map(_._2 ).sum
    val currentNumberPurchases = prices.size
    val state = currentTotal.getOrElse((0,0.0))
    Some((currentNumberPurchases + state._1 , currentRevenue + state._2 ))
  }
}