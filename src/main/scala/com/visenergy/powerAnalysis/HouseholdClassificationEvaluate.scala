package com.visenergy.powerAnalysis

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.impurity.Gini
import org.json.JSONArray
import org.json.JSONObject
import com.visenergy.emulator.RedisDao
import java.text.SimpleDateFormat
/**
 * 分类：四种求分类进行评估
 */
object HouseholdClassificationEvaluate{

  def main(args: Array[String]): Unit = {
    var sparkServer = "local[2]"
    if(args.length > 0){
      sparkServer = args(0)
    }
    val sparkConf = new SparkConf().setAppName("HouseholdClassificationEvaluate").setMaster(sparkServer)
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
    //简单ETL
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
      var la = 0.0
      if(hd(8).toFloat >= 16)
        la = 1.0
      ds(2).toDouble :: (ds(1).toDouble :: (ds(0).toDouble :: (ts(0).toDouble :: (ts(1).toDouble :: (la ::Nil)))))
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
    //预测参数
    val numIterations = 10
    val maxTreeDepth = 20
    val numTotal = vdata.count
    
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
    redisClient.publish("mllib_evalInit_time", (lrPredictStart - start).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_evalInit_time", (lrPredictStart - start).toDouble/1000 + " s"))
  
    val lrResult = ClassificationUtil.logicEvaluate(vdata,vdata, numTotal, numIterations)
    redisClient.publish("mllib_lr_accuracy", addColor(lrResult._1  * 100))
    redisClient.publish("mllib_lr_pr",  addColor(lrResult._2  * 100 ))
    redisClient.publish("mllib_lr_roc", addColor(lrResult._3  * 100 ))
    redisJsonArray.put(new JSONObject().put("mllib_lr_accuracy", addColor(lrResult._1  * 100)))
    redisJsonArray.put(new JSONObject().put("mllib_lr_pr",  addColor(lrResult._2  * 100 )))
    redisJsonArray.put(new JSONObject().put("mllib_lr_roc", addColor(lrResult._3  * 100 )))
    outputStringList ::= "lrResult:" + lrResult
    
    //svm
    val svmPredictStart = System.currentTimeMillis()
    redisClient.publish("mllib_lrEval_time", (svmPredictStart - lrPredictStart).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_lrEval_time", (svmPredictStart - lrPredictStart).toDouble/1000 + " s"))

    val svmResult = ClassificationUtil.svmEvaluate(vdata,vdata, numTotal, numIterations)
    redisClient.publish("mllib_svm_accuracy", addColor(svmResult._1  * 100 ))
    redisClient.publish("mllib_svm_pr",  addColor(svmResult._2  * 100 ))
    redisClient.publish("mllib_svm_roc", addColor(svmResult._3  * 100 ))
    redisJsonArray.put(new JSONObject().put("mllib_svm_accuracy", addColor(svmResult._1  * 100 )))
    redisJsonArray.put(new JSONObject().put("mllib_svm_pr",  addColor(svmResult._2  * 100 )))
    redisJsonArray.put(new JSONObject().put("mllib_svm_roc", addColor(svmResult._3  * 100 )))
    outputStringList ::= "svmResult:" + svmResult
    
    //朴素贝叶斯
    val nbPredictStart = System.currentTimeMillis()
    redisClient.publish("mllib_svmEval_time", (nbPredictStart - svmPredictStart).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_svmEval_time", (nbPredictStart - svmPredictStart).toDouble/1000 + " s"))

    val nbResult = ClassificationUtil.naiveBayesEvaluate(vdata,vdata, numTotal, numIterations)
    redisClient.publish("mllib_nb_accuracy", addColor(nbResult._1  * 100 ))
    redisClient.publish("mllib_nb_pr",  addColor(nbResult._2  * 100 ))
    redisClient.publish("mllib_nb_roc", addColor(nbResult._3  * 100 ))
    redisJsonArray.put(new JSONObject().put("mllib_nb_accuracy", addColor(nbResult._1  * 100 )))
    redisJsonArray.put(new JSONObject().put("mllib_nb_pr",  addColor(nbResult._2  * 100 )))
    redisJsonArray.put(new JSONObject().put("mllib_nb_roc", addColor(nbResult._3  * 100 )))
    outputStringList ::= "nbResult:" + nbResult
    
    //决策树
    val dtPredictStart = System.currentTimeMillis()
    redisClient.publish("mllib_nbEval_time", (dtPredictStart - nbPredictStart).toDouble/1000 + " s")

    val dtResult = ClassificationUtil.decisionTreeEvaluate(vdata,vdata,numTotal, Gini, maxTreeDepth)
    redisClient.publish("mllib_dt_accuracy", addColor(dtResult._1  * 100 ))
    redisClient.publish("mllib_dt_pr",  addColor(dtResult._2  * 100 ))
    redisClient.publish("mllib_dt_roc", addColor(dtResult._3  * 100 ))
    redisJsonArray.put(new JSONObject().put("mllib_dt_accuracy", addColor(dtResult._1  * 100 )))
    redisJsonArray.put(new JSONObject().put("mllib_dt_pr",  addColor(dtResult._2  * 100 )))
    redisJsonArray.put(new JSONObject().put("mllib_dt_roc", addColor(dtResult._3  * 100 )))
    outputStringList ::= "dtResult:" + dtResult
   
    val redictEnd = System.currentTimeMillis()
    redisClient.publish("mllib_dtEval_time", (redictEnd - dtPredictStart).toDouble/1000 + " s")
    redisClient.publish("mllib_evalAll_time", (redictEnd - start).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_dtEval_time", (redictEnd - dtPredictStart).toDouble/1000 + " s"))
    redisJsonArray.put(new JSONObject().put("mllib_evalAll_time", (redictEnd - start).toDouble/1000 + " s"))
    
    redisClient.setnx("HouseholdClassificationEvaluate_"+startTime.replaceAll(":", "#")+"_"+endTime.replaceAll(":", "#"), redisJsonArray.toString())

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