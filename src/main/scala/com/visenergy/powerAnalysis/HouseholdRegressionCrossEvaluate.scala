package com.visenergy.powerAnalysis

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
import java.text.SimpleDateFormat
import com.visenergy.emulator.RedisDao
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.json.JSONArray
import org.json.JSONObject

/**
 * 家庭用电交叉预测
 */
object HouseholdRegressionCrossEvaluate{

  def main(args: Array[String]): Unit = {
    var sparkServer = "local[2]"
    if(args.length > 0){
      sparkServer = args(0)
    }
    val sparkConf = new SparkConf().setAppName("HouseholdEvaluate").setMaster(sparkServer)
    val sc = new SparkContext(sparkConf)
    
    val start = System.currentTimeMillis()
    val hdData = sc.textFile("test/household/household1.txt")
    //val hdData = sc.textFile("hdfs://node1:9000/household/household.txt")
    //数据ETL
    val hdData2 = hdData.filter(d => !d.contains("?")).map(d => d.split(";"))
    
    val hdData3 = hdData2.map{ hd =>
      val ds = hd(0).split("/")
      val ts = hd(1).split(":")
      
      ds(2).toDouble :: (ds(1).toDouble :: (ds(0).toDouble :: (ts(0).toDouble :: (ts(1).toDouble :: (hd(2).toDouble :: Nil)))))
    }
    val mappings = ClassificationUtil.initBinaryVector(hdData3,5)
    var trainData = hdData3
    var testData = hdData3
    val type_train = 1
    val args1 = Array("60%","40%")
    //val args1 = Array("2006,2007,2008,2009","2010")
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
    
    val evaluateStart = System.currentTimeMillis()
    redisClient.publish("mllib_cross_evalInit_time", (evaluateStart - start).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_cross_evalInit_time", (evaluateStart - start).toDouble/1000 + " s"))
    
    def addColor(num:Double) : String = {
      if(num == 0.0){
        "<span class='blueFont'>" + num + "</span>"
      }else if(num > 0.0 && num <0.3){
        "<span class='greenFont'>" + num + "</span>"
      }else{
        "<span class='redFont'>" + num + "</span>"
      }
    }
    //逻辑回归
    val logicEvaluateResult = RegressionUtil.logicEvaluate(vTrainData,vTestData)
    
    redisClient.publish("mllib_cross_lr_mse", addColor(logicEvaluateResult._1))
    redisClient.publish("mllib_cross_lr_mae", addColor(logicEvaluateResult._2))
    redisClient.publish("mllib_cross_lr_rmsle", addColor(logicEvaluateResult._3))
    redisJsonArray.put(new JSONObject().put("mllib_cross_lr_mse", addColor(logicEvaluateResult._1)))
    redisJsonArray.put(new JSONObject().put("mllib_cross_lr_mae", addColor(logicEvaluateResult._2)))
    redisJsonArray.put(new JSONObject().put("mllib_cross_lr_rmsle", addColor(logicEvaluateResult._3)))
    
    val lrEnd = System.currentTimeMillis()
    redisClient.publish("mllib_cross_lr_time",  (lrEnd - evaluateStart).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_cross_lr_time",  (lrEnd - evaluateStart).toDouble/1000 + " s"))
    
    //决策树
    val dtEvaluateResult = RegressionUtil.decisionTreeEvaluate(vTrainData,vTestData)
    redisClient.publish("mllib_cross_dt_mse", addColor(dtEvaluateResult._1))
    redisClient.publish("mllib_cross_dt_mae", addColor(dtEvaluateResult._2))
    redisClient.publish("mllib_cross_dt_rmsle", addColor(dtEvaluateResult._3))
    redisJsonArray.put(new JSONObject().put("mllib_cross_dt_mse", addColor(dtEvaluateResult._1)))
    redisJsonArray.put(new JSONObject().put("mllib_cross_dt_mae", addColor(dtEvaluateResult._2)))
    redisJsonArray.put(new JSONObject().put("mllib_cross_dt_rmsle", addColor(dtEvaluateResult._3)))
    
    val dtEnd = System.currentTimeMillis()
    redisClient.publish("mllib_cross_dt_time",  (dtEnd - lrEnd).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_cross_dt_time",  (dtEnd - lrEnd).toDouble/1000 + " s"))
    
    
    //自定义方法验证
    val selfTrainData = trainData.map{record =>
    	(record(3)*60 + record(4),record(5))
    }
    val selfTestData = testData.map{record =>
    	(record(3)*60 + record(4),record(5))
    }
    
    val tData = selfTrainData.combineByKey((v)=>(v,1), (acc:(Double,Int),v)=>(acc._1+v,acc._2+1), (acc1:(Double,Int),acc2:(Double,Int))=>(acc1._1+acc2._1,acc1._2+acc2._2))
	val tData2 =tData.map{case(key,value) => (key,(value._1 /value._2).toDouble)}
    val tDataMap = tData2.collectAsMap()
    val selfResult = RegressionUtil.evaluateResult(selfTestData.map{case(k,v) => (v,tDataMap(k))})
    redisClient.publish("mllib_cross_self_mse", addColor(selfResult._1))
    redisClient.publish("mllib_cross_self_mae", addColor(selfResult._2))
    redisClient.publish("mllib_cross_self_rmsle", addColor(selfResult._3))
    redisJsonArray.put(new JSONObject().put("mllib_cross_self_mse", addColor(selfResult._1)))
    redisJsonArray.put(new JSONObject().put("mllib_cross_self_mae", addColor(selfResult._2)))
    redisJsonArray.put(new JSONObject().put("mllib_cross_self_rmsle", addColor(selfResult._3)))
    
    val selfEnd = System.currentTimeMillis()
    redisClient.publish("mllib_cross_self_time",  (selfEnd - dtEnd).toDouble/1000 + " s")
    redisClient.publish("mllib_cross_evalAll_time", (selfEnd - start).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_cross_self_time",  (selfEnd - dtEnd).toDouble/1000 + " s"))
    redisJsonArray.put(new JSONObject().put("mllib_cross_evalAll_time", (selfEnd - start).toDouble/1000 + " s"))

    redisClient.setnx("HouseholdRegressionCrossEvaluate_"+type_train+"_"+args1(0).replaceAll(":", "#")+"_"+args1(1).replaceAll(":", "#"), redisJsonArray.toString())

    outputStringList ::= "time:" + (dtEnd - start).toDouble/1000
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