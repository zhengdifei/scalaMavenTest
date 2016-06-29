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
 * 家庭用电预测
 */
object HouseholdRegressionEvaluate{

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
    
    //时间
    var startTime:String = "2006-01-01 00:00"
    var endTime:String = "2010-12-31 00:00"
	if(args.length == 2){
	    startTime = args(0)
	    endTime = args(1)
	}
    //数据ETL
    val hdData2 = hdData.filter(d => !d.contains("?")).map(d => d.split(";"))
    //按时间过滤
    val hdData3 = hdData2.filter{ d =>
      val sdf1 = new SimpleDateFormat("yyyy-MM-dd hh:mm")
      val stMilis = sdf1.parse(startTime).getTime()
      val etMilis = sdf1.parse(endTime).getTime()
      val sdf2 = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss")
      val dMilis = sdf2.parse(d(0) + " "+ d(1)).getTime()
      dMilis >= stMilis && dMilis <= etMilis
    }
    
    val hdData4 = hdData3.map{ hd =>
      val ds = hd(0).split("/")
      val ts = hd(1).split(":")
      
      ds(2).toDouble :: (ds(1).toDouble :: (ds(0).toDouble :: (ts(0).toDouble :: (ts(1).toDouble :: (hd(2).toDouble :: Nil)))))
    }
    val mappings = ClassificationUtil.initBinaryVector(hdData4,5)
    val vdata = ClassificationUtil.transformBinaryVector(hdData4,5,6,mappings)
    vdata.cache
    //输出
    var outputStringList = List[String]()
    //redis初始化
    RedisDao.makePool()
    val redisClient = RedisDao.getPool.getResource()
    
    //存入redis的JSONObject对象
    val redisJsonArray = new JSONArray()
    
    val evaluateStart = System.currentTimeMillis()
    redisClient.publish("mllib_evalInit_time", (evaluateStart - start).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_evalInit_time", (evaluateStart - start).toDouble/1000 + " s"))

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
    val logicEvaluateResult = RegressionUtil.logicEvaluate(vdata,vdata)
    
    redisClient.publish("mllib_lr_mse", addColor(logicEvaluateResult._1 ))
    redisClient.publish("mllib_lr_mae", addColor(logicEvaluateResult._2 ))
    redisClient.publish("mllib_lr_rmsle", addColor(logicEvaluateResult._3 ))
    redisJsonArray.put(new JSONObject().put("mllib_lr_mse", addColor(logicEvaluateResult._1 )))
    redisJsonArray.put(new JSONObject().put("mllib_lr_mae", addColor(logicEvaluateResult._2 )))
    redisJsonArray.put(new JSONObject().put("mllib_lr_rmsle", addColor(logicEvaluateResult._3 )))

    val lrEnd = System.currentTimeMillis()
    redisClient.publish("mllib_lr_time",  (lrEnd - evaluateStart).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_lr_time",  (lrEnd - evaluateStart).toDouble/1000 + " s"))
    
    //决策树
    val dtEvaluateResult = RegressionUtil.decisionTreeEvaluate(vdata,vdata)
    redisClient.publish("mllib_dt_mse", addColor(dtEvaluateResult._1 ))
    redisClient.publish("mllib_dt_mae", addColor(dtEvaluateResult._2 ))
    redisClient.publish("mllib_dt_rmsle", addColor(dtEvaluateResult._3 ))
    redisJsonArray.put(new JSONObject().put("mllib_dt_mse", addColor(dtEvaluateResult._1 )))
    redisJsonArray.put(new JSONObject().put("mllib_dt_mae", addColor(dtEvaluateResult._2 )))
    redisJsonArray.put(new JSONObject().put("mllib_dt_rmsle", addColor(dtEvaluateResult._3 )))    
    
    val dtEnd = System.currentTimeMillis()
    redisClient.publish("mllib_dt_time",  (dtEnd - lrEnd).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_dt_time",  (dtEnd - lrEnd).toDouble/1000 + " s"))   
    
    //自定义方法验证
    val selfData1 = hdData4.map{record =>
    	(record(3)*60 + record(4),record(5))
    }
    
    val selfData2 = selfData1.combineByKey((v)=>(v,1), (acc:(Double,Int),v)=>(acc._1+v,acc._2+1), (acc1:(Double,Int),acc2:(Double,Int))=>(acc1._1+acc2._1,acc1._2+acc2._2))
	val selfData3 =selfData2.map{case(key,value) => (key,(value._1 /value._2).toDouble)}
    val selfDataMap = selfData3.collectAsMap()
    val selfResult = RegressionUtil.evaluateResult(selfData1.map{case(k,v) => (v,selfDataMap(k))})
    redisClient.publish("mllib_self_mse", addColor(selfResult._1 ))
    redisClient.publish("mllib_self_mae", addColor(selfResult._2 ))
    redisClient.publish("mllib_self_rmsle", addColor(selfResult._3 ))
    redisJsonArray.put(new JSONObject().put("mllib_self_mse", addColor(selfResult._1 )))
    redisJsonArray.put(new JSONObject().put("mllib_self_mae", addColor(selfResult._2 )))
    redisJsonArray.put(new JSONObject().put("mllib_self_rmsle", addColor(selfResult._3 )))        
    
    val selfEnd = System.currentTimeMillis()
    redisClient.publish("mllib_self_time",  (selfEnd - dtEnd).toDouble/1000 + " s")
    redisClient.publish("mllib_evalAll_time", (selfEnd - start).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_self_time",  (selfEnd - dtEnd).toDouble/1000 + " s"))
    redisJsonArray.put(new JSONObject().put("mllib_evalAll_time", (selfEnd - start).toDouble/1000 + " s"))
    
    redisClient.setnx("HouseholdRegressionEvaluate_"+startTime.replaceAll(":", "#")+"_"+endTime.replaceAll(":", "#"), redisJsonArray.toString())

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