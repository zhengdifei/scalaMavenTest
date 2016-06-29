package com.visenergy.MLlib

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import breeze.linalg._
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

object MonitorStreamingModel {

  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext("local[2]","Streaming app",Seconds(10))
    
    val stream = ssc.socketTextStream("localhost", 9999)
    
    val NumFeatures = 100
    val zeroVector = DenseVector.zeros[Double](NumFeatures)
    val model1 = new StreamingLinearRegressionWithSGD()
    	.setInitialWeights(Vectors.dense(zeroVector.data ))
    	.setNumIterations(1)
    	.setStepSize(0.01)
    val model2 = new StreamingLinearRegressionWithSGD()
    	.setInitialWeights(Vectors.dense(zeroVector.data ))
    	.setNumIterations(1)
    	.setStepSize(1.0)
    
    val labeledStream = stream.map{ event =>
      val split = event.split("\t")
      val y = split(0).toDouble
      val features = split(1).split(",").map(_.toDouble)
      LabeledPoint(label = y,features = Vectors.dense(features))
    }
    
    model1.trainOn(labeledStream)
    model2.trainOn(labeledStream)
    
    val predsAndTree = labeledStream.transform{ rdd =>
      val latest1 = model1.latestModel
      val latest2 = model2.latestModel
      rdd.map{ point =>
        val pred1 = latest1.predict(point.features)
        val pred2 = latest2.predict(point.features)
        (pred1 - point.label ,pred2 - point.label )
      }
    }
    
    predsAndTree.foreachRDD{ (rdd,time) =>
      val mse1 = rdd.map{ case(err1,err2) => err1 * err1}.mean()
      val rmse1 = math.sqrt(mse1)
      
      val mse2 = rdd.map{ case(err1,err2) => err2 * err2}.mean()
      val rmse2 = math.sqrt(mse2)
      
      println(s"-------------------Time:$time".stripMargin)
      println(s"MSE current batch:Model1:$mse1 ;Model2: $mse2")
      println(s"RMSE current batch:Model1:$rmse1 ;Model2: $rmse2")
      println("\n")
    }
    
    ssc.start
    ssc.awaitTermination
  }

}