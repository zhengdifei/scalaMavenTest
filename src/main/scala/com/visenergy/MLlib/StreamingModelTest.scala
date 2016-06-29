package com.visenergy.MLlib

import breeze.linalg._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

object StreamingModelTest {

  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext("local[2]","Streaming App",Seconds(10))
    
    val stream = ssc.socketTextStream("localhost", 9999)
    
    val NumFeatures = 100
    val zeroVector = DenseVector.zeros[Double](NumFeatures)
    val model = new StreamingLinearRegressionWithSGD()
    	.setInitialWeights(Vectors.dense(zeroVector.data))
    	.setNumIterations(1)
    	.setStepSize(0.01)
    
    val labeledStream = stream.map{ event => 
      val split = event.split("\t")
      val y = split(0).toDouble
      val features = split(1).split(",").map(_.toDouble)
      LabeledPoint(label = y,features = Vectors.dense(features))
    }
    
    val labeledVector = stream.map{ event => 
      val split = event.split("\t")
      val y = split(0).toDouble
      val features = split(1).split(",").map(_.toDouble)
      (Vectors.dense(features))
    }
    
    model.trainOn(labeledStream)
    model.predictOn(labeledVector).print()
    
    ssc.start
    ssc.awaitTermination
  }

}