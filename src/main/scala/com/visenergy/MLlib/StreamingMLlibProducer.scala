package com.visenergy.MLlib

import scala.util.Random
import java.net.ServerSocket
import java.io.PrintWriter
import java.io.FileInputStream

object StreamingMLlibProducer {

  def main(args: Array[String]): Unit = {
    val random = new Random()
    
    val MaxEvents = 6
    
    val namesResource = new FileInputStream("test/names.csv")//this.getClass().getResourceAsStream("names.csv")
    
    val names = scala.io.Source.fromInputStream(namesResource).getLines.toList.head.split(",").toSeq
    
    val products = Seq(
        "iPhone Cover" -> 9.99,
        "Headphones" -> 5.49,
        "Samsung Galaxy Cover" -> 8.95,
        "Ipad Cover" -> 7.49
     )
     
     def generateProductEvents(n : Int) = {
	    (1 to n).map{ i =>
	      val (product,price) = products(random.nextInt(products.size))
	      val user = random.shuffle(names).head
	      (user,product,price)
	    }
	  }
    
    val listener = new ServerSocket(9999)
    println("Listening on port:9999")
    
    while(true){
      val socket = listener.accept()
      new Thread(){
        override def run = {
          println("Got client connected from:" + socket.getInetAddress())
          val out = new PrintWriter(socket.getOutputStream(),true)
          
          while(true){
            Thread.sleep(1000)
            val num = random.nextInt(MaxEvents)
            val productEvents = generateProductEvents(num)
            productEvents.foreach{ event =>
            	out.write(event._1 + "," + event._2 + "," + event._3)
            	out.write("\n")
            }
            out.flush()
            println(s"Created $num events……")
          }
          socket.close()
        }
      }.start()
    }
  }
}