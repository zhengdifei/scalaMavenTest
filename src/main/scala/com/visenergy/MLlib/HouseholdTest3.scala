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
import org.apache.spark.mllib.optimization.SimpleUpdater
import org.apache.spark.mllib.optimization.SquaredL2Updater
import org.apache.spark.mllib.tree.impurity.Impurity
import org.apache.spark.mllib.tree.impurity.Gini
import org.apache.hadoop.mapreduce.v2.app.webapp.NavBlock
import org.apache.spark.mllib.tree.RandomForest
/**
 * 交叉测试,选择使用nb，决策树进行交叉验证
 */
object HouseholdTest3 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HouseholdTest").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    
    val hdData = sc.textFile("test/household/household1.txt")
    
    val hdData2 = hdData.filter(d => !d.contains("?")).map(d => d.split(";"))
    //println(hdData2.first())
    
    val hdData3 = hdData2.map{ hd =>
      val ds = hd(0).split("/")
      val ts = hd(1).split(":")
      var la = 0
      if(hd(8).toFloat >= 16)
        la = 1
      
     //(ds(2).toInt,ds(1).toInt,ds(0).toInt,ts(0).toInt,ts(1).toInt,la)

      (ds(2).toInt,ds(1).toInt,ds(0).toInt,ts(0).toInt,ts(1).toInt,la,hd(2).toDouble,hd(3).toDouble,hd(4).toDouble,hd(5).toDouble,hd(6).toDouble,hd(7).toDouble)
    }
    
    var mappings= List[Map[Int,Long]]()
      
    mappings ::= hdData3.map(a => a._5).distinct.zipWithIndex.collectAsMap()
    mappings ::= hdData3.map(a => a._4).distinct.zipWithIndex.collectAsMap()
    mappings ::= hdData3.map(a => a._3).distinct.zipWithIndex.collectAsMap()
    mappings ::= hdData3.map(a => a._2).distinct.zipWithIndex.collectAsMap()
    mappings ::= hdData3.map(a => a._1).distinct.zipWithIndex.collectAsMap()
    
    //总长度
    var cat_len = 0
    for(i <- 0 to mappings.length-1){
      cat_len += mappings(i).size
    }

    def extract_feature(record:Tuple12[Int,Int,Int,Int,Int,Int,Double,Double,Double,Double,Double,Double]) = {

    //def extract_feature(record:Tuple6[Int,Int,Int,Int,Int,Int]) = {
//    	val cat_vec = new Array[Double](cat_len)
//    	for(i <- 0 to cat_len-1){
//    	  cat_vec(i) = 0
//    	}
        val cat_vec = Array.ofDim[Double](cat_len)
    	var step = 0
    	for(a <- 0 to 4){
    	  val m = mappings(a)
    	  var idx = 0
    	  if(a == 0) idx = m(record._1).toInt
    	  else if(a == 1) idx = m(record._2).toInt
    	  else if(a == 2) idx = m(record._3).toInt
    	  else if(a == 3) idx = m(record._4).toInt
    	  else if(a == 4) idx = m(record._5).toInt
    	  
    	  cat_vec(idx + step) = 1
    	  step += m.size
    	}
    	//cat_vec
        val other_vec = Array[Double](record._7,record._8,record._9,record._10,record._11,record._12)
    	cat_vec ++ other_vec
    }
    
    val vdata = hdData3.map( record => LabeledPoint(record._6,Vectors.dense(extract_feature(record))))
//    val first_vdata = vdata.first
//    println("label:" + first_vdata.label )
//    println("feature:" + first_vdata.features )
    
    var outputStringList = List[String]()
   
    val trainTestSplit = vdata.randomSplit(Array(0.6,0.4), 123)
    val train = trainTestSplit(0)
    val test = trainTestSplit(1)
    /*
     * nb测试
     * 0.001 lambda param,AUC:66.8374%
     * 0.01 lambda param,AUC:66.8374%
     * 0.1 lambda param,AUC:66.8375%
     * 1.0 lambda param,AUC:66.8372%
     * 10.0 lambda param,AUC:66.8330%
     * 
     * 添加了6个属性
     * 0.001 lambda param,AUC:74.2796%
     * 0.01 lambda param,AUC:74.2796%
     * 0.1 lambda param,AUC:74.2796%
     * 1.0 lambda param,AUC:74.2792%
     * 10.0 lambda param,AUC:74.2772%
     */
    val nbResults = Seq(0.001,0.01,0.1,1.0,10.0).map{ param =>
    	val model = trainNbWithParams(train,param)
    	
    	val scoreAndLabels = test.map{ point => 
    		(model.predict(point.features),point.label)
    	}
    
    	val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    	(s"$param lambda param",metrics.areaUnderROC)
    }
    nbResults.foreach{ case (label,auc) =>
      outputStringList ::= f"$label,AUC:${auc * 100}%2.4f%%"
    }
    /*
     * 决策树深度测试
     * 1 tree depth param,AUC:53.3321%
     * 2 tree depth param,AUC:56.6328%
     * 3 tree depth param,AUC:59.1839%
     * 4 tree depth param,AUC:61.6432%
     * 5 tree depth param,AUC:63.9636%
     * 10 tree depth param,AUC:66.2083%
     * 20 tree depth param,AUC:73.3432%
     *
     * 添加6个属性
     * 1 tree depth param,AUC:92.4640%
     * 2 tree depth param,AUC:92.4640%
     * 3 tree depth param,AUC:92.8184%
     * 4 tree depth param,AUC:93.5351%
     * 5 tree depth param,AUC:93.2753%
     * 10 tree depth param,AUC:94.6831%
     * 20 tree depth param,AUC:97.4778%
     */
    val dtDepthResults = Seq(1,2,3,4,5,10,20).map{ param =>
    	val model = trainDtWithParams(vdata,param,Gini)
    	
    	val scoreAndLabels = test.map{ point => 
    		(model.predict(point.features),point.label)
    	}
    
    	val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    	(s"$param tree depth param",metrics.areaUnderROC)
    }
    dtDepthResults.foreach{ case (label,auc) =>
      outputStringList ::= f"$label,AUC:${auc * 100}%2.4f%%"
    }
    /*
     *  随机深林（添加6个属性之后）
     *  
     *  1 tree depth param,AUC:89.3987%
     *  2 tree depth param,AUC:93.2201%
     *  3 tree depth param,AUC:92.8224%
     *  4 tree depth param,AUC:92.4489%
     *  5 tree depth param,AUC:91.8748%
     *  10 tree depth param,AUC:62.2524%
     *  20 tree depth param,AUC:93.0810%
     */
//    val rfDepthResults = Seq(1,2,3,4,5,10,20).map{ param =>
//    	val model = RandomForest.trainClassifier(vdata,2,scala.collection.immutable.Map[Int, Int](),3,"auto","gini",5,100)
//    	
//    	val scoreAndLabels = test.map{ point => 
//    		(model.predict(point.features),point.label)
//    	}
//    
//    	val metrics = new BinaryClassificationMetrics(scoreAndLabels)
//    	(s"$param tree depth param",metrics.areaUnderROC)
//    }
//    rfDepthResults.foreach{ case (label,auc) =>
//      outputStringList ::= f"$label,AUC:${auc * 100}%2.4f%%"
//    }
    //逆序
    //outputStringList.foreach(println)
    //正序输出
    var n = outputStringList.length - 1
    while(n>=0){
      println(outputStringList(n))
      n = n -1 
    }
  }
  /*
   * 逻辑回归不同参数测试
   */
  def trainWithParams(input : RDD[LabeledPoint],regParam:Double,numIterations:Int,updater:Updater,stepSize:Double) = {
    val lr = new LogisticRegressionWithSGD
    lr.optimizer.setNumIterations(numIterations).setUpdater(updater).setRegParam(regParam).setStepSize(stepSize)
    lr.run(input)
  }
  
  def createMetrics(label : String, data : RDD[LabeledPoint], model : ClassificationModel) =  {
    val scoreAndLabels = data.map{ point => 
    	(model.predict(point.features),point.label)
    }
    
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    (label,metrics.areaUnderROC)
  }
  /*
   * 决策树不同参数测试
   */
  def trainDtWithParams(input : RDD[LabeledPoint],maxDepth : Int,impurity : Impurity) = {
    DecisionTree.train(input, Algo.Classification, impurity, maxDepth)
  }
  /*
   * lambe测试
   */
  def trainNbWithParams(input : RDD[LabeledPoint],lambda : Double) = {
    val nb = new NaiveBayes
    nb.setLambda(lambda)
    nb.run(input)
  }
}