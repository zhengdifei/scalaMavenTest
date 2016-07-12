package com.visenergy.MLlib

import breeze.linalg._
import scala.util.Random
import java.net.ServerSocket
import java.io.PrintWriter

object StreamingModelProducer {

  def main(args: Array[String]): Unit = {
    val MaxEvents = 100
    val NumFeatures = 100
    val random = new Random()
    
    def generateRandomArray(n : Int) = Array.tabulate(n)(_ => random.nextGaussian())
    
    val w = new DenseVector(generateRandomArray(NumFeatures))
    val intercept = random.nextGaussian() * 10
    
    def generateNoisyData(n : Int) = {
      (1 to n).map{ i =>
        val x = new DenseVector(generateRandomArray(NumFeatures))
        val y : Double = w.dot(x)
        val noisy = y + intercept
        (noisy,x)
      }
    }
    
    val listener = new ServerSocket(9999)
    println("Listening on port : 9999")
    
    while(true) {
      val socket = listener.accept()
      new Thread(){
        override def run = {
          println("Got client connected from :" + socket.getInetAddress())
          val out = new PrintWriter(socket.getOutputStream(),true)
          
          while(true){
            Thread.sleep(1000)
            val num = random.nextInt(MaxEvents)
            val data = generateNoisyData(num)
            data.foreach{ case(y,x) =>
              val xStr = x.data.mkString(",")
              val eventStr = s"$y\t$xStr"
              out.write(eventStr)
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