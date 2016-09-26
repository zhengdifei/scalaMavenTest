package com.visenergy.powerAnalysis

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.impurity.Gini
import com.visenergy.emulator.RedisDao
import org.json.JSONArray
import org.json.JSONObject
/**
 * 分类：四种求分类进行评估
 */
object HouseholdClassificationCrossEvaluate{

  def main(args: Array[String]): Unit = {
    var sparkServer = "local[2]"
    if(args.length > 0){
      sparkServer = args(0)
    }
    val sparkConf = new SparkConf().setAppName("HouseholdClassificationCrossEvaluate").setMaster(sparkServer)
    val sc = new SparkContext(sparkConf)
    
    val start = System.currentTimeMillis()
    
    val hdData = sc.textFile("test/household/household1.txt")
    //val hdData = sc.textFile("hdfs://node1:9000/household/household.txt")
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
    var trainData = hdData3
    var testData = hdData3
    val type_train = 2
    //val args1 = Array("60%","40%")
    val args1 = Array("2007,2008,2009","2010")
    if(type_train == 1){     
      var trainNum = args1(0).substring(0,args1(0).length()-1).toDouble/100
      var testNum = args1(1).substring(0,args1(1).length()-1).toDouble/100
      val trainTestData = hdData3.randomSplit(Array(trainNum,testNum))
      trainData = trainTestData(0)
      testData = trainTestData(1)
    }else{      
      val trainNum = args1(0).split(",")
      val testNum = args1(1).split(",")
      trainData = hdData3.filter{record =>
      	var bool = false
        for(i <- 0 to trainNum.length-1){
      	  if(record(0) == trainNum(i).toDouble) bool = true
      	}
      	bool
      }
      
      testData = hdData3.filter{record =>
      	var bool = false
        for(i <- 0 to testNum.length-1){
      	  if(record(0) == testNum(i).toDouble) bool = true
      	}
      	bool
      }
    }
    val vTrainData = ClassificationUtil.transformBinaryVector(trainData,5,6,mappings)
    val vTestData = ClassificationUtil.transformBinaryVector(testData,5,6,mappings)
    vTrainData.cache
    vTestData.cache
    //输出
    var outputStringList = List[String]()
    //redis初始化
    RedisDao.makePool()
    val redisClient = RedisDao.getPool.getResource()
    
    //存入redis的JSONObject对象
    val redisJsonArray = new JSONArray()
    //预测参数
    val numIterations = 10
    val maxTreeDepth = 20
    val numTotal = vTestData.count
    
    def addColor(num:Double) : String = {
      if(num == 100.0){
        "<span class='blueFont'>" + num + "</span>"
      }else if(num >= 75 && num <100){
        "<span class='greenFont'>" + num + "</span>"
      }else if(num <= 25){
        "<span class='greenFont'>" + (100 - num) + "</span>"
      }else if(num > 25 && num < 50){
        "<span class='redFont'>" + (100 - num) + "</span>"
      }else{
        "<span class='redFont'>" + num + "</span>"
      }
    }
    //逻辑回归
    val lrPredictStart = System.currentTimeMillis()
    redisClient.publish("mllib_cross_evalInit_time", (lrPredictStart - start).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_cross_evalInit_time", (lrPredictStart - start).toDouble/1000 + " s"))
  
    val lrResult = ClassificationUtil.logicEvaluate(vTrainData,vTestData,numTotal, numIterations)
    redisClient.publish("mllib_cross_lr_accuracy", addColor(lrResult._1  * 100 ))
    redisClient.publish("mllib_cross_lr_pr",  addColor(lrResult._2  * 100 ))
    redisClient.publish("mllib_cross_lr_roc", addColor(lrResult._3  * 100 ))
    redisJsonArray.put(new JSONObject().put("mllib_cross_lr_accuracy", addColor(lrResult._1  * 100 )))
    redisJsonArray.put(new JSONObject().put("mllib_cross_lr_pr",  addColor(lrResult._2  * 100 )))
    redisJsonArray.put(new JSONObject().put("mllib_cross_lr_roc", addColor(lrResult._3  * 100 )))
    
    outputStringList ::= "lrResult:" + lrResult
    
    //svm
    val svmPredictStart = System.currentTimeMillis()
    redisClient.publish("mllib_cross_lrEval_time", (svmPredictStart - lrPredictStart).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_cross_lrEval_time", (svmPredictStart - lrPredictStart).toDouble/1000 + " s"))
    
    val svmResult = ClassificationUtil.svmEvaluate(vTrainData,vTestData, numTotal, numIterations)
    redisClient.publish("mllib_cross_svm_accuracy", addColor(svmResult._1  * 100 ))
    redisClient.publish("mllib_cross_svm_pr",  addColor(svmResult._2  * 100 ))
    redisClient.publish("mllib_cross_svm_roc", addColor(svmResult._3  * 100 ))
    redisJsonArray.put(new JSONObject().put("mllib_cross_svm_accuracy", addColor(svmResult._1  * 100 )))
    redisJsonArray.put(new JSONObject().put("mllib_cross_svm_pr",  addColor(svmResult._2  * 100 )))
    redisJsonArray.put(new JSONObject().put("mllib_cross_svm_roc", addColor(svmResult._3  * 100 )))
    
    outputStringList ::= "svmResult:" + svmResult
    
    //朴素贝叶斯
    val nbPredictStart = System.currentTimeMillis()
    redisClient.publish("mllib_cross_svmEval_time", (nbPredictStart - svmPredictStart).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_cross_svmEval_time", (nbPredictStart - svmPredictStart).toDouble/1000 + " s"))

    val nbResult = ClassificationUtil.naiveBayesEvaluate(vTrainData,vTestData,numTotal, numIterations)
    redisClient.publish("mllib_cross_nb_accuracy", addColor(nbResult._1  * 100 ))
    redisClient.publish("mllib_cross_nb_pr",  addColor(nbResult._2  * 100 ))
    redisClient.publish("mllib_cross_nb_roc", addColor(nbResult._3  * 100 ))
    redisJsonArray.put(new JSONObject().put("mllib_cross_nb_accuracy", addColor(nbResult._1  * 100 )))
    redisJsonArray.put(new JSONObject().put("mllib_cross_nb_pr",  addColor(nbResult._2  * 100 )))
    redisJsonArray.put(new JSONObject().put("mllib_cross_nb_roc", addColor(nbResult._3  * 100 )))
    
    outputStringList ::= "nbResult:" + nbResult
    
    //决策树
    val dtPredictStart = System.currentTimeMillis()
    redisClient.publish("mllib_cross_nbEval_time", (dtPredictStart - nbPredictStart).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_cross_nbEval_time", (dtPredictStart - nbPredictStart).toDouble/1000 + " s"))

    val dtResult = ClassificationUtil.decisionTreeEvaluate(vTrainData,vTestData, numTotal, Gini, maxTreeDepth)
    redisClient.publish("mllib_cross_dt_accuracy", addColor(dtResult._1  * 100 ))
    redisClient.publish("mllib_cross_dt_pr",  addColor(dtResult._2  * 100 ))
    redisClient.publish("mllib_cross_dt_roc", addColor(dtResult._3  * 100 ))
    redisJsonArray.put(new JSONObject().put("mllib_cross_dt_accuracy", addColor(dtResult._1  * 100 )))
    redisJsonArray.put(new JSONObject().put("mllib_cross_dt_pr",  addColor(dtResult._2  * 100 )))
    redisJsonArray.put(new JSONObject().put("mllib_cross_dt_roc", addColor(dtResult._3  * 100 )))
    
    outputStringList ::= "dtResult:" + dtResult
   
    val redictEnd = System.currentTimeMillis()
    redisClient.publish("mllib_cross_dtEval_time", (redictEnd - dtPredictStart).toDouble/1000 + " s")
    redisClient.publish("mllib_cross_evalAll_time", (redictEnd - start).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_cross_dtEval_time", (redictEnd - dtPredictStart).toDouble/1000 + " s"))
    redisJsonArray.put(new JSONObject().put("mllib_cross_evalAll_time", (redictEnd - start).toDouble/1000 + " s"))
    
    redisClient.setnx("HouseholdClassificationCrossEvaluate_"+type_train+"_"+args1(0).replaceAll(":", "#")+"_"+args1(1).replaceAll(":", "#"), redisJsonArray.toString())

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