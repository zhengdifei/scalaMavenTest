package com.visenergy.MLlib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.Map
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.impurity.Entropy
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.optimization.Updater
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.classification.ClassificationModel
import org.apache.spark.mllib.tree.RandomForest
import com.visenergy.powerAnalysis.ClassificationUtil
/**
 * 分类：四种求分类的方法
 */
object HouseholdTest {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HouseholdTest").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    
    val start = System.currentTimeMillis()
    
    val hdData = sc.textFile("test/household/household1.txt")
    
    val hdData2 = hdData.filter(d => !d.contains("?")).map(d => d.split(";"))
    //println(hdData2.first())
    
    val hdData3 = hdData2.map{ hd =>
      val ds = hd(0).split("/")
      val ts = hd(1).split(":")
      var la = 0.0
      if(hd(8).toFloat >= 16)
        la = 1.0
      
      //(ds(2).toInt,ds(1).toInt,ds(0).toInt,ts(0).toInt,ts(1).toInt,la)

      //(ds(2).toInt,ds(1).toInt,ds(0).toInt,ts(0).toInt,ts(1).toInt,la,hd(2).toDouble,hd(3).toDouble,hd(4).toDouble,hd(5).toDouble,hd(6).toDouble,hd(7).toDouble)
      ds(2).toDouble :: (ds(1).toDouble :: (ds(0).toDouble :: (ts(0).toDouble :: (ts(1).toDouble :: (la ::Nil)))))
    }
    
    var mappings= List[Map[Double,Long]]()
    for(i <- 0 to 4){
      mappings ::= hdData3.map(a => a(4-i)).distinct.zipWithIndex.collectAsMap()
    }
//    mappings ::= hdData3.map(a => a._5).distinct.zipWithIndex.collectAsMap()
//    mappings ::= hdData3.map(a => a._4).distinct.zipWithIndex.collectAsMap()
//    mappings ::= hdData3.map(a => a._3).distinct.zipWithIndex.collectAsMap()
//    mappings ::= hdData3.map(a => a._2).distinct.zipWithIndex.collectAsMap()
//    mappings ::= hdData3.map(a => a._1).distinct.zipWithIndex.collectAsMap()
    
    //总长度
    var cat_len = 0
    for(i <- 0 to mappings.length-1){
      cat_len += mappings(i).size
    }
    
    def extract_feature(record:List[Double]) = {
//    def extract_feature(record:Tuple12[Int,Int,Int,Int,Int,Int,Double,Double,Double,Double,Double,Double]) = {

//    def extract_feature(record:Tuple6[Int,Int,Int,Int,Int,Int]) = {
//    	val cat_vec = new Array[Double](cat_len)
//    	for(i <- 0 to cat_len-1){
//    	  cat_vec(i) = 0
//    	}
        val cat_vec = Array.ofDim[Double](cat_len)
    	var step = 0
    	for(a <- 0 to 4){
    	  val m = mappings(a)
    	  val idx = m(record(a)).toInt
//    	  if(a == 0) idx = m(record._1).toInt
//    	  else if(a == 1) idx = m(record._2).toInt
//    	  else if(a == 2) idx = m(record._3).toInt
//    	  else if(a == 3) idx = m(record._4).toInt
//    	  else if(a == 4) idx = m(record._5).toInt
    	  
    	  cat_vec(idx + step) = 1
    	  step += m.size
    	}
        
//        cat_vec
        val other_vec = Array[Double](record(7),record(8),record(9),record(10),record(11),record(12))
    	cat_vec ++ other_vec
    }
    //输出
    var outputStringList = List[String]()
    
//    val vdata = hdData3.map( record => LabeledPoint(record(5),Vectors.dense(extract_feature(record))))
    val vdata = ClassificationUtil.transformBinaryVector(hdData3,5,6,Nil)
    val first_vdata = vdata.first
    outputStringList ::= "label:" + first_vdata.label
    outputStringList ::= "feature:" + first_vdata.features
    
   
    //举证统计数据
    val vectors = vdata.map(hd => hd.features )
    val matrix = new RowMatrix(vectors)
    val matrixSummary = matrix.computeColumnSummaryStatistics()
    //println(matrixSummary.mean)
    
    
    val numIterations = 10
    val maxTreeDepth = 5
    val numTotal = vdata.count
    //逻辑回归
//    val lrModel = LogisticRegressionWithSGD.train(vdata, numIterations)
//    val lrTotalCorrect = vdata.map{ point => 
//    	if(lrModel.predict(point.features) == point.label) 1 else 0
//    }.sum
//    val lrAccuracy = lrTotalCorrect/numTotal
//    //println("lrAccuracy:" + lrAccuracy)
//    outputStringList ::= "lrAccuracy:" + lrAccuracy
    
    //svm
//    val svmModel = SVMWithSGD.train(vdata, numIterations)
//    val svmTotalCorrect = vdata.map{ point => 
//    	if(svmModel.predict(point.features) == point.label) 1 else 0
//    }.sum
//    val svmAccuracy = svmTotalCorrect/numTotal
//    //println("svmAccuracy:" + svmAccuracy)
//    outputStringList ::= "svmAccuracy:" + svmAccuracy
    
    //朴素贝叶斯
//    val nbModel = NaiveBayes.train(vdata)
//    val nbTotalCorrect = vdata.map{ point => 
//    	if(nbModel.predict(point.features) == point.label) 1 else 0
//    }.sum
//    val nbAccuracy = nbTotalCorrect/numTotal
//    //println("nbAccuracy:" + nbAccuracy)
//    outputStringList ::= "nbAccuracy:" + nbAccuracy
    
    //决策树
    val dtModel = DecisionTree.train(vdata,Algo.Classification,Entropy,20)
    var a = sc.accumulator(0,"a")
    var b = sc.accumulator(0,"b")
    var c = sc.accumulator(0,"c")
    var d = sc.accumulator(0,"d")
    val dtTotalCorrect = vdata.map{ point => 
        val score = dtModel.predict(point.features)
        if(score ==1.0 && point.label ==1.0) a +=1//println("a:"+score + ":" + point.label)
        if(score ==1.0 && point.label ==0.0) b +=1//println("b:"+score + ":" + point.label)
        if(score ==0.0 && point.label ==0.0) c +=1//println("c:"+score + ":" + point.label)
        if(score ==0.0 && point.label ==1.0) d +=1//println("d:"+score + ":" + point.label)
    	if(score == point.label) 1 else 0
    }.sum
    val dtAccuracy = dtTotalCorrect/numTotal
    //println("dtAccuracy:" + dtAccuracy)
    outputStringList ::= "dtAccuracy:" + dtAccuracy
    outputStringList ::= "a:" + a.value + ",p:" + a.value/numTotal
    outputStringList ::= "b:" + b.value + ",p:" + b.value/numTotal
    outputStringList ::= "c:" + c.value + ",p:" + c.value/numTotal
    outputStringList ::= "d:" + d.value + ",p:" + d.value/numTotal
    outputStringList ::= "total:" + numTotal
    //随机深林
//    val rfModel = RandomForest.trainClassifier(vdata,2,scala.collection.immutable.Map[Int, Int](),4,"auto","gini",5,100)
//    val rfTotalCorrect = vdata.map{ point => 
//        val score = rfModel.predict(point.features)
//    	if(score == point.label) 1 else 0
//    }.sum
//    val rfAccuracy = rfTotalCorrect/numTotal
//    //println("dtAccuracy:" + dtAccuracy)
//    outputStringList ::= "rfAccuracy:" + rfAccuracy
//    //ROC
//    val metrics = Seq(lrModel,svmModel).map{ model =>
//    	val scoreAndLabels = vdata.map{ point =>
//    		(model.predict(point.features),point.label)
//    	}
//    	val metrics = new BinaryClassificationMetrics(scoreAndLabels)
//    	(model.getClass().getSimpleName(),metrics.areaUnderPR,metrics.areaUnderROC)
//    }
//    
//    val nbMetrics = Seq(nbModel).map{ model =>
//    	val scoreAndLabels = vdata.map{ point =>
//    		(model.predict(point.features),point.label)
//    	}
//    	val metrics = new BinaryClassificationMetrics(scoreAndLabels)
//    	(model.getClass().getSimpleName(),metrics.areaUnderPR,metrics.areaUnderROC)
//    }
//    
//     val dtMetrics = Seq(dtModel).map{ model =>
//    	val scoreAndLabels = vdata.map{ point =>
//    		(model.predict(point.features),point.label)
//    	}
//    	val metrics = new BinaryClassificationMetrics(scoreAndLabels)
//    	(model.getClass().getSimpleName(),metrics.areaUnderPR,metrics.areaUnderROC)
//    }
//    
//    val allMetrics = metrics ++ nbMetrics ++ dtMetrics
//    allMetrics.foreach{ case (m,pr,roc) =>
//      outputStringList ::= f"$m,PR:${pr * 100.0}%2.4f%%,ROC:${roc * 100.0}%2.4f%%"
//    }
    val end = System.currentTimeMillis()
    outputStringList ::= "time:" + (end - start)
    //逆序
    //outputStringList.foreach(println)
    //正序输出
    var n = outputStringList.length - 1
    while(n>=0){
      println(outputStringList(n))
      n = n -1 
    }
  }
}