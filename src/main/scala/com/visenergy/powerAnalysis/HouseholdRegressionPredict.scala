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
object HouseholdRegressionPredict{

  def main(args: Array[String]): Unit = {
    var sparkServer = "local[2]"
    if(args.length > 0){
      sparkServer = args(0)
    }
    val sparkConf = new SparkConf().setAppName("HouseholdPredict").setMaster(sparkServer)
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
    hdData3.cache
    
    val mappings = ClassificationUtil.initBinaryVector(hdData3,5)
    val vdata = ClassificationUtil.transformBinaryVector(hdData3,5,6,mappings)
    vdata.cache
    //输出
    var outputStringList = List[String]()
   
    RedisDao.makePool()
    val redisClient = RedisDao.getPool.getResource()

    //存入redis的JSONObject对象
    val redisJsonArray = new JSONArray()
    
    val args1 = Array("2006-12-16 17:24","2009-6-30 1:28","2008-5-8 4:49","2007-9-30 5:9","2006-2-8 10:23")
    //val args1 = Array("2006-12-16 17:24","2006-12-16 17:51","2006-12-16 18:00","2006-12-16 18:55","2006-12-16 20:37")

    //实际值
    val acPredictStart = System.currentTimeMillis()
    redisClient.publish("mllib_predictInit_time", (acPredictStart - start).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_predictInit_time", (acPredictStart - start).toDouble/1000 + " s"))

    val actualMap:scala.collection.mutable.Map[Int,Double] = scala.collection.mutable.Map()
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
	      actualMap += (i -> 0.0)
	      outputStringList ::= args1(i) +": 不存在日期"
	      redisClient.publish("mllib_actualNum_"+(i+1), "未知，日期不存在")
	      redisJsonArray.put(new JSONObject().put("mllib_actualNum_"+(i+1), "未知，日期不存在"))
	    }else if(numRdd == 1){
	      actualMap += (i -> avalueRdd.first()(5))
	      outputStringList ::= args1(i) + ":" +avalueRdd.first()(5)
	      redisClient.publish("mllib_actualNum_"+(i+1), avalueRdd.first()(5) + "")
	      redisJsonArray.put(new JSONObject().put("mllib_actualNum_"+(i+1), avalueRdd.first()(5) + ""))
	    }else{
	      actualMap += (i -> avalueRdd.first()(5))
	      redisClient.publish("mllib_actualNum_"+(i+1), "有"+ numRdd +"")
	      redisJsonArray.put(new JSONObject().put("mllib_actualNum_"+(i+1), "有"+ numRdd +""))
	    }
    }
    /*
     * 线性回归
     */
    val lrPredictStart = System.currentTimeMillis()
    redisClient.publish("mllib_acPredict_time", (lrPredictStart - acPredictStart).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_acPredict_time", (lrPredictStart - acPredictStart).toDouble/1000 + " s"))
    
    val lrModel = LinearRegressionWithSGD.train(vdata, 10,0.1)
    for(i <- 0 to args1.length-1){
	    val datetime = args1(i).split(" ")
	    val ds = datetime(0).split("-")
        val ts = datetime(1).split(":")
        val pvec = Vectors.dense(ClassificationUtil.extractFeature(ds(0).toDouble :: (ds(1).toDouble :: (ds(2).toDouble :: (ts(0).toDouble :: (ts(1).toDouble :: Nil)))),5,mappings))
        val pvalue = lrModel.predict(pvec)
        var deviationRate = ""
        if(actualMap(i) != 0){
          deviationRate = "误差率：<span style='color:red'>" + CommonUtil.reserveDecimals(math.abs(pvalue - actualMap(i))/actualMap(i) * 100,3) + "%</span>"
        }
        outputStringList ::= args1(i) +":"+pvalue
        redisClient.publish("mllib_lr_predictNum_"+(i+1), pvalue + "&nbsp;&nbsp;&nbsp;&nbsp;" + deviationRate)
        redisJsonArray.put(new JSONObject().put("mllib_lr_predictNum_"+(i+1), pvalue + "&nbsp;&nbsp;&nbsp;&nbsp;" + deviationRate))
    }
    //决策树
    val dtPredictStart = System.currentTimeMillis()
    redisClient.publish("mllib_lrPredict_time", (dtPredictStart - lrPredictStart).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_lrPredict_time", (dtPredictStart - lrPredictStart).toDouble/1000 + " s"))

    
    val dtModel = DecisionTree.trainRegressor(vdata,scala.collection.immutable.Map[Int, Int](),"variance",30,100)
    for(i <- 0 to args1.length-1){
	    val datetime = args1(i).split(" ")
	    val ds = datetime(0).split("-")
        val ts = datetime(1).split(":")
        val pvec = Vectors.dense(ClassificationUtil.extractFeature(ds(0).toDouble :: (ds(1).toDouble :: (ds(2).toDouble :: (ts(0).toDouble :: (ts(1).toDouble :: Nil)))),5,mappings))
        val pvalue = dtModel.predict(pvec)
        var deviationRate = ""
        if(actualMap(i) != 0){
          deviationRate = "误差率：<span style='color:red'>" + CommonUtil.reserveDecimals(math.abs(pvalue - actualMap(i))/actualMap(i) * 100,3) + "%</span>"
        }	    
	    outputStringList ::= args1(i) +":"+pvalue
        redisClient.publish("mllib_dt_predictNum_"+(i+1), pvalue + "&nbsp;&nbsp;&nbsp;&nbsp;" + deviationRate)
        redisJsonArray.put(new JSONObject().put("mllib_dt_predictNum_"+(i+1), pvalue + "&nbsp;&nbsp;&nbsp;&nbsp;" + deviationRate))
	}

    //随机森林
    val rfPredictStart = System.currentTimeMillis()
    redisClient.publish("mllib_dtPredict_time", (rfPredictStart - dtPredictStart).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_dtPredict_time", (rfPredictStart - dtPredictStart).toDouble/1000 + " s"))

    
//    val rfModel = RandomForest.trainRegressor(vdata,scala.collection.immutable.Map[Int, Int](),3,"auto","variance",20,100)
//    for(i <- 0 to args1.length-1){
//	    val datetime = args1(i).split(" ")
//	    val ds = datetime(0).split("-")
//        val ts = datetime(1).split(":")
//        val pvec = Vectors.dense(ClassificationUtil.extractFeature(ds(0).toDouble :: (ds(1).toDouble :: (ds(2).toDouble :: (ts(0).toDouble :: (ts(1).toDouble :: Nil)))),5,mappings))
//        val pvalue = rfModel.predict(pvec)
//        var deviationRate = ""
//        if(actualMap(i) != 0){
//          deviationRate = "误差率：<span style='color:red'>" + CommonUtil.reserveDecimals(math.abs(pvalue - actualMap(i))/actualMap(i) * 100,3) + "%</span>"
//        }	    
//	    outputStringList ::= args1(i) +":"+pvalue
//        redisClient.publish("mllib_rf_predictNum_"+(i+1), pvalue + "&nbsp;&nbsp;&nbsp;&nbsp;" + deviationRate)
//        redisJsonArray.put(new JSONObject().put("mllib_rf_predictNum_"+(i+1), pvalue + "&nbsp;&nbsp;&nbsp;&nbsp;" + deviationRate))
//	}
    val rfPredictEnd = System.currentTimeMillis()
    redisClient.publish("mllib_rfPredict_time", (rfPredictEnd - rfPredictStart).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_rfPredict_time", (rfPredictEnd - rfPredictStart).toDouble/1000 + " s"))

    //自定义方法
    val selfData1 = hdData3.map{record =>
    	(record(3)*60 + record(4),record(5))
    }
    
    val selfData2 = selfData1.combineByKey((v)=>(v,1), (acc:(Double,Int),v)=>(acc._1+v,acc._2+1), (acc1:(Double,Int),acc2:(Double,Int))=>(acc1._1+acc2._1,acc1._2+acc2._2))
	val selfData3 =selfData2.map{case(key,value) => (key,(value._1 /value._2).toDouble)}
    for(i <- 0 to args1.length-1){
	    val datetime = args1(i).split(" ")
	    val ds = datetime(0).split("-")
        val ts = datetime(1).split(":")
        val pvalue = selfData3.lookup(ts(0).toDouble*60 + ts(1).toDouble)(0)
        var deviationRate = ""
        if(actualMap(i) != 0){
          deviationRate = "误差率：<span style='color:red'>" + CommonUtil.reserveDecimals(math.abs(pvalue - actualMap(i))/actualMap(i) * 100,3) + "%</span>"
        }	    
	    outputStringList ::= args1(i) +":"+pvalue
        redisClient.publish("mllib_self_predictNum_"+(i+1), pvalue + "&nbsp;&nbsp;&nbsp;&nbsp;" + deviationRate)
        redisJsonArray.put(new JSONObject().put("mllib_self_predictNum_"+(i+1), pvalue + "&nbsp;&nbsp;&nbsp;&nbsp;" + deviationRate))
	}
    val selfPredictEnd = System.currentTimeMillis()
    redisClient.publish("mllib_selfPredict_time", (selfPredictEnd - rfPredictEnd).toDouble/1000 + " s")
    redisClient.publish("mllib_predictAll_time", (selfPredictEnd - start).toDouble/1000 + " s")
    redisJsonArray.put(new JSONObject().put("mllib_selfPredict_time", (selfPredictEnd - rfPredictEnd).toDouble/1000 + " s"))
    redisJsonArray.put(new JSONObject().put("mllib_predictAll_time", (selfPredictEnd - start).toDouble/1000 + " s"))
    
    redisClient.setnx("HouseholdRegressionPredict_"+args1(0).replaceAll(":", "#")+"_"+args1(1).replaceAll(":", "#")+"_"+args1(2).replaceAll(":", "#")+"_"+args1(3).replaceAll(":", "#")+"_"+args1(4).replaceAll(":", "#"), redisJsonArray.toString())

    RedisDao.getPool.returnResource(redisClient)
    
    outputStringList ::= "time:" + (selfPredictEnd - start).toDouble/1000
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