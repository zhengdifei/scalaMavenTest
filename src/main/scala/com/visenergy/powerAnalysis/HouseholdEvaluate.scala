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
import org.apache.spark.mllib.tree.impurity.Gini
import com.visenergy.emulator.RedisDao
/**
 * 分类：四种求分类进行评估
 */
object HouseholdEvaluate{

  def main(args: Array[String]): Unit = {
    var sparkServer = "local[2]"
    if(args.length > 0){
      sparkServer = args(0)
    }
    val sparkConf = new SparkConf().setAppName("HouseholdClassificationEvaluate").setMaster(sparkServer)
    val sc = new SparkContext(sparkConf)
    
    val start = System.currentTimeMillis()
    
    
    val hdData = sc.textFile("hdfs://node1:9000/household/household.txt")
    //简单ETL
    val hdData2 = hdData.filter(d => !d.contains("?")).map(d => d.split(";"))
    
    val hdData3 = hdData2.map{ hd =>
      val ds = hd(0).split("/")
      val ts = hd(1).split(":")
      var la = 0.0
      if(hd(8).toFloat >= 16)
        la = 1.0
      ds(2).toDouble :: (ds(1).toDouble :: (ds(0).toDouble :: (ts(0).toDouble :: (ts(1).toDouble :: (la ::Nil)))))
    }
    
    val mappings = ClassificationUtil.initBinaryVector(hdData3,5)
    val vdata = ClassificationUtil.transformBinaryVector(hdData3,5,6,mappings)
    vdata.cache
    //输出
    var outputStringList = List[String]()
    //redis初始化
    RedisDao.makePool()
    val redisClient = RedisDao.getPool.getResource()
    
    //预测参数
    val numIterations = 10
    val maxTreeDepth = 5
    val numTotal = vdata.count
    //逻辑回归
    val lrPredictStart = System.currentTimeMillis()
    val lrResult = ClassificationUtil.logicEvaluate(vdata,vdata, numTotal, numIterations)
    redisClient.publish("mllib_lr_accurary", lrResult._1  * 100 +"")
    redisClient.publish("mllib_lr_pr",  lrResult._2  * 100 +"")
    redisClient.publish("mllib_lr_roc", lrResult._3  * 100 +"")
    outputStringList ::= "lrResult:" + lrResult
    
    //svm
    val svmResult = ClassificationUtil.svmEvaluate(vdata,vdata, numTotal, numIterations)
    redisClient.publish("mllib_lr_accurary", svmResult._1  * 100 +"")
    redisClient.publish("mllib_lr_pr",  svmResult._2  * 100 +"")
    redisClient.publish("mllib_lr_roc", svmResult._3  * 100 +"")
    outputStringList ::= "svmResult:" + svmResult
    
    //朴素贝叶斯
    val nbResult = ClassificationUtil.naiveBayesEvaluate(vdata,vdata, numTotal, numIterations)
    redisClient.publish("mllib_lr_accurary", nbResult._1  * 100 +"")
    redisClient.publish("mllib_lr_pr",  nbResult._2  * 100 +"")
    redisClient.publish("mllib_lr_roc", nbResult._3  * 100 +"")
    outputStringList ::= "nbResult:" + nbResult
    
    //决策树
    val dtResult = ClassificationUtil.decisionTreeEvaluate(vdata,vdata, numTotal, Gini, maxTreeDepth)
    redisClient.publish("mllib_lr_accurary", dtResult._1  * 100 +"")
    redisClient.publish("mllib_lr_pr",  dtResult._2  * 100 +"")
    redisClient.publish("mllib_lr_roc", dtResult._3  * 100 +"")
    outputStringList ::= "dtResult:" + dtResult
   
    val redictEnd = System.currentTimeMillis()
    redisClient.publish("mllib_predictAll_time", (redictEnd - start).toDouble/1000 + " s")
    outputStringList ::= "time:" + (redictEnd - start).toDouble/1000
    RedisDao.getPool.returnResource(redisClient)
    
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