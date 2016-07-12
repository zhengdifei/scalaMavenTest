package com.visenergy.MLlib

import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary,Statistics}
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.SVMWithSGD

object MTest {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("MLlibTest").setMaster("local")
    val sc = new SparkContext(sparkConf)
	val array1 : Array[Double] = Array[Double](60,70,80,0)
	val array2 : Array[Double] = Array[Double](80,50,0,90)
	val array3 : Array[Double] = Array[Double](60,70,80,0)
	val denseArray1 = Vectors.dense(array1)
	val denseArray2 = Vectors.dense(array2)
	val denseArray3 = Vectors.dense(array3)
	
	val seqDenseArray:Seq[Vector] = Seq(denseArray1,denseArray2,denseArray3)
	val basicTestRdd : RDD[Vector] = sc.parallelize[Vector](seqDenseArray)
	//统计操作
	val summary : MultivariateStatisticalSummary = Statistics.colStats(basicTestRdd)
	println("avg:" + summary.mean)
	println("fc:" + summary.variance)
	println("not zero:" + summary.numNonzeros)
	
	//分析相关性
	val morningStudyTime : RDD[Double] = sc.parallelize(List(55, 54, 60, 60, 45, 20, 85, 40), 2)
	val nightStudyTime : RDD[Double] = sc.parallelize(List(80, 90, 80, 90, 70, 20, 100, 60), 2)
	val corrType = "pearson"
	val corr : Double = Statistics.corr(morningStudyTime, nightStudyTime,corrType)
	println(s"Correlation: \t $corr")
	
	//KMeans聚类
	val dataFile = "test/kmeans_data.txt"
	val lines = sc.textFile(dataFile)
	
	val data = lines.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()
	val numCluster = 2
	val numIterations = 20
	val model = KMeans.train(data,numCluster,numIterations)
//	println("Final centers:" + model.clusterCenters)
//	println("Total cost:" + model.computeCost(data))
	//输出模型的聚集中心
	val WSSSE = model.computeCost(data)
	println(s"Within Set Sum of Squared Errors = $WSSSE")
	model.clusterCenters.foreach(v => 
		for(i <- v.toArray){
		  print(i+"\t")
		}
	)
	
	//SVM算法
	val dataFile2 = "test/sample_libsvm_data.txt"
	val data2 = MLUtils.loadLibSVMFile(sc,dataFile2)
	
	val splits = data2.randomSplit(Array(0.8,0.2), seed = 9L)
	val training = splits(0).cache
	val test = splits(1)
	
	//打印分割后的数据量
	println("TraninigCount:" + training.count)
	println("TraninigTest:" + test.count)
	
	val model2 = SVMWithSGD.train(training,100)
	model2.clearThreshold()
	
	val scoreAndLabels = test.map{ point => 
		val score = model2.predict(point.features )
		println(score + "\t" + point.label)
		(score, point.label )
    }
    
    scoreAndLabels.collect()
  }
}