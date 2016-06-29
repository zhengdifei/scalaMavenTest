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
import com.visenergy.powerAnalysis.ClassificationUtil
import java.text.SimpleDateFormat
import com.visenergy.emulator.RedisDao
import org.apache.spark.mllib.optimization.SimpleUpdater
import org.apache.spark.mllib.tree.impurity.Gini
import org.json.JSONArray
import org.json.JSONObject
/**
 * 分类：四种求分类的方法进行预测
 */
object HouseholdClassificationPredict{

  def main(args: Array[String]): Unit = {
    var sparkServer = "local[2]"
    if(args.length > 0){
      sparkServer = args(0)
    }
    val sparkConf = new SparkConf().setAppName("HouseholdClassificationPredict").setMaster(sparkServer)
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
    val vdata = ClassificationUtil.transformBinaryVector(hdData3,5,6,mappings)
    vdata.cache
    
    //输出
    var outputStringList = List[String]()
    //redis初始化
    RedisDao.makePool()
    val redisClient = RedisDao.getPool.getResource()
    
    //存入redis的JSONObject对象
    val redisJsonArray = new JSONArray()
    //预测参数
    val args1 = Array("2006-12-16 17:24","2009-6-30 1:28","2008-5-8 4:49","2007-9-30 5:9","2006-2-8 10:23")
    //val args1 = Array("2006-12-16 17:24","2006-12-16 17:51","2006-12-16 18:00","2006-12-16 18:55","2006-12-16 20:37")
    
    val numIterations = 10
    val maxTreeDepth = 20
    val numTotal = vdata.count
    //实际值
    val acPredictStart = System.currentTimeMillis()
    redisClient.publish("mllib_predictInit_time", (acPredictStart - start).toDouble/1000 + " s")
	redisJsonArray.put(new JSONObject().put("mllib_predictInit_time", (acPredictStart - start).toDouble/1000 + " s"))

    for(i <- 0 to args1.length-1){
	    val datetime = args1(i).split(" ")
	    val ds = datetime(0).split("-")
        val ts = datetime(1).split(":")
        val avec = ds(0).toDouble :: (ds(1).toDouble :: (ds(2).toDouble :: (ts(0).toDouble :: (ts(1).toDouble :: Nil))))
        val avalueRdd = hdData3.filter{d =>
	      if(d(0) == avec(0) && d(1) == avec(1) && d(2) == avec(2) && d(3) == avec(3) && d(4) == avec(4)) true else false
	    }
	    val numRdd = avalueRdd.count()
	    if( numRdd== 0){
	      outputStringList ::= args1(i) +": 不存在日期"
	      redisClient.publish("mllib_actualNum_"+(i+1), "未知，日期不存在")
	      redisJsonArray.put(new JSONObject().put("mllib_actualNum_"+(i+1), "未知，日期不存在"))
	    }else if(numRdd == 1){
	      outputStringList ::= args1(i) + ":" +avalueRdd.first()(5)
	      if(avalueRdd.first()(5) == 1.0){
	    	  redisClient.publish("mllib_actualNum_"+(i+1), "<span style='color:green'>开</span>")
	    	  redisJsonArray.put(new JSONObject().put("mllib_actualNum_"+(i+1), "<span style='color:green'>开</span>"))	      
	      }else{
	    	  redisClient.publish("mllib_actualNum_"+(i+1), "<span style='color:red'>关</span>")
	    	  redisJsonArray.put(new JSONObject().put("mllib_actualNum_"+(i+1), "<span style='color:red'>关</span>"))	      
	      }
	    }else{
	      redisClient.publish("mllib_actualNum_"+(i+1), "有"+ numRdd +"个重复时间")
	      redisJsonArray.put(new JSONObject().put("mllib_actualNum_"+(i+1), "有"+ numRdd +"个重复时间"))	      
	    }
    }
    //逻辑回归
    val lrPredictStart = System.currentTimeMillis()
    redisClient.publish("mllib_acPredict_time", (lrPredictStart - acPredictStart).toDouble/1000 + " s")
	redisJsonArray.put(new JSONObject().put("mllib_acPredict_time", (lrPredictStart - acPredictStart).toDouble/1000 + " s"))	      

    val lrModel = ClassificationUtil.getLogicModel(vdata,numIterations,0.0,1.0,new SimpleUpdater)
    for(i <- 0 to args1.length-1){
	    val datetime = args1(i).split(" ")
	    val ds = datetime(0).split("-")
        val ts = datetime(1).split(":")
        val pvec = Vectors.dense(ClassificationUtil.extractFeature(ds(0).toDouble :: (ds(1).toDouble :: (ds(2).toDouble :: (ts(0).toDouble :: (ts(1).toDouble :: Nil)))),5,mappings))
        val pvalue = if(lrModel.predict(pvec) == 0.0) "<span style='color:red'>关</span>" else "<span style='color:green'>开</span>"
        outputStringList ::= args1(i) +":"+pvalue
        redisClient.publish("mllib_lr_predictNum_"+(i+1), pvalue + "")
        redisJsonArray.put(new JSONObject().put("mllib_lr_predictNum_"+(i+1), pvalue + ""))	      
    }
    //svm
    val svmPredictStart = System.currentTimeMillis()
    redisClient.publish("mllib_lrPredict_time", (svmPredictStart - lrPredictStart).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_lrPredict_time", (svmPredictStart - lrPredictStart).toDouble/1000 + " s"))	      

    val svmModel = SVMWithSGD.train(vdata, numIterations)
    for(i <- 0 to args1.length-1){
	    val datetime = args1(i).split(" ")
	    val ds = datetime(0).split("-")
        val ts = datetime(1).split(":")
        val pvec = Vectors.dense(ClassificationUtil.extractFeature(ds(0).toDouble :: (ds(1).toDouble :: (ds(2).toDouble :: (ts(0).toDouble :: (ts(1).toDouble :: Nil)))),5,mappings))
        val pvalue = if(svmModel.predict(pvec)== 0.0) "<span style='color:red'>关</span>" else "<span style='color:green'>开</span>"
        outputStringList ::= args1(i) +":"+pvalue
        redisClient.publish("mllib_svm_predictNum_"+(i+1), pvalue + "")
        redisJsonArray.put(new JSONObject().put("mllib_svm_predictNum_"+(i+1), pvalue + ""))	      
    }
    
    //朴素贝叶斯
    val nbPredictStart = System.currentTimeMillis()
    redisClient.publish("mllib_svmPredict_time", (nbPredictStart - svmPredictStart).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_svmPredict_time", (nbPredictStart - svmPredictStart).toDouble/1000 + " s"))	      

    val nbModel = ClassificationUtil.getNaiveBayesModel(vdata, 0.1)
    for(i <- 0 to args1.length-1){
	    val datetime = args1(i).split(" ")
	    val ds = datetime(0).split("-")
        val ts = datetime(1).split(":")
        val pvec = Vectors.dense(ClassificationUtil.extractFeature(ds(0).toDouble :: (ds(1).toDouble :: (ds(2).toDouble :: (ts(0).toDouble :: (ts(1).toDouble :: Nil)))),5,mappings))
        val pvalue = if(nbModel.predict(pvec)== 0.0) "<span style='color:red'>关</span>" else "<span style='color:green'>开</span>"
        outputStringList ::= args1(i) +":"+pvalue
        redisClient.publish("mllib_nb_predictNum_"+(i+1), pvalue + "")
        redisJsonArray.put(new JSONObject().put("mllib_nb_predictNum_"+(i+1), pvalue + ""))	      
    }
    
    //决策树
    val dtPredictStart = System.currentTimeMillis()
    redisClient.publish("mllib_nbPredict_time", (dtPredictStart - nbPredictStart).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_nbPredict_time", (dtPredictStart - nbPredictStart).toDouble/1000 + " s"))	      

    val dtResult = DecisionTree.train(vdata, Algo.Classification, Gini, maxTreeDepth)
    for(i <- 0 to args1.length-1){
	    val datetime = args1(i).split(" ")
	    val ds = datetime(0).split("-")
        val ts = datetime(1).split(":")
        val pvec = Vectors.dense(ClassificationUtil.extractFeature(ds(0).toDouble :: (ds(1).toDouble :: (ds(2).toDouble :: (ts(0).toDouble :: (ts(1).toDouble :: Nil)))),5,mappings))
        val pvalue = if(dtResult.predict(pvec)== 0.0) "<span style='color:red'>关</span>" else "<span style='color:green'>开</span>"
        outputStringList ::= args1(i) +":"+pvalue
        redisClient.publish("mllib_dt_predictNum_"+(i+1), pvalue + "")
        redisJsonArray.put(new JSONObject().put("mllib_dt_predictNum_"+(i+1), pvalue + ""))	      
    }
   
    val redictEnd = System.currentTimeMillis()
    redisClient.publish("mllib_dtPredict_time", (redictEnd - dtPredictStart).toDouble/1000 + " s")
    redisClient.publish("mllib_predictAll_time", (redictEnd - start).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_dtPredict_time", (redictEnd - dtPredictStart).toDouble/1000 + " s"))	      
    redisJsonArray.put(new JSONObject().put("mllib_predictAll_time", (redictEnd - start).toDouble/1000 + " s"))	      
    
    redisClient.setnx("HouseholdClassificationPredict_"+args1(0).replaceAll(":", "#")+"_"+args1(1).replaceAll(":", "#")+"_"+args1(2).replaceAll(":", "#")+"_"+args1(3).replaceAll(":", "#")+"_"+args1(4).replaceAll(":", "#"), redisJsonArray.toString())
    
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