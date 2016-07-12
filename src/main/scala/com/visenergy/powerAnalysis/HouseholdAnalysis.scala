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
import org.json.JSONObject
import org.json.JSONArray

/**
 * 家庭用电分析
 */
object HouseholdAnalysis{

  def main(args: Array[String]): Unit = {
    var sparkServer = "local[2]"
    if(args.length > 0){
      sparkServer = args(0)
    }
    val sparkConf = new SparkConf().setAppName("HouseholdAnalysis").setMaster(sparkServer)
    val sc = new SparkContext(sparkConf)
    
    val start = System.currentTimeMillis()
    
    val hdData = sc.textFile("test/household/household1.txt")
    //val hdData = sc.textFile("hdfs://node1:9000/household/household.txt")
    
    //时间
    var startTime:String = "2007-01-01"
    var endTime:String = "2007-12-31"
	if(args.length == 2){
	    startTime = args(0)
	    endTime = args(1)
	}
    
    //数据ETL
    val hdData2 = hdData.filter(d => !d.contains("?")).map(d => d.split(";"))
    //按时间过滤
    val hdData3 = hdData2.filter{ d =>
      val sdf1 = new SimpleDateFormat("yyyy-MM-dd")
      val stMilis = sdf1.parse(startTime).getTime()
      val etMilis = sdf1.parse(endTime).getTime()
      val sdf2 = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss")
      val dMilis = sdf2.parse(d(0) + " "+ d(1)).getTime()
      dMilis >= stMilis && dMilis <= etMilis
    }
    
    val hdData4 = hdData3.map{ hd =>
      val ds = hd(0).split("/")
      val ts = hd(1).split(":")
      
      ds(2).toDouble :: (ds(1).toDouble :: (ds(0).toDouble :: (ts(0).toDouble :: (ts(1).toDouble :: (hd(2).toDouble :: (hd(3).toDouble :: (hd(4).toDouble :: (hd(5).toDouble ::(hd(6).toDouble :: (hd(7).toDouble :: (hd(8).toDouble :: Nil)))))))))))
    }
    hdData4.cache
    //根据partition求中位数
    def partitionMidNum(input:RDD[Double], msgPos:Long) : Double = {
	    input.coalesce(1).sortBy(x=>x, true).mapPartitions { 
	      iter => {
	         var result = List[Double]()
	         var pos = 1L
			     while (iter.hasNext) {
			       val number = iter.next()
			       if (pos == msgPos) {
			         result ::= number
			       }
			       pos += 1
			     }
	    	   result.iterator
	      }
	    }.collect().apply(0)
	}
    //输出
    var outputStringList = List[String]()
    val activeMeanKw = hdData4.map(record => record(5)).mean
    val reactiveMeanKW = hdData4.map(record => record(6)).mean
    val allKwh = hdData4.map(record => record(5)).sum/60
    val meanKwh = activeMeanKw /60
    val midPos = (hdData4.count() + 1)/2
    val activeMidKw =  partitionMidNum(hdData4.map(record => record(5)), midPos)
      
    //将月份1变成01格式
    def transformMonth(month:Int):String = {
      if(month <10){
        "0" + month
      }else{
        "" + month
      }
    }
    val activeByMonth = hdData4.map{record => (record(0).toInt+"年"+transformMonth(record(1).toInt)+"月",record(5))}.reduceByKey(_+_).map{ case (n,v) => (n,CommonUtil.reserveDecimals(v/60, 2))}.sortByKey()
    val monthArr = activeByMonth.keys.collect.mkString(",")
    val activeArr = activeByMonth.values.collect.mkString(",")
    val reactiveByMonth = hdData4.map{record => (record(0).toInt+"年"+transformMonth(record(1).toInt)+"月",record(6))}.reduceByKey(_+_).map{ case (n,v) => (n,CommonUtil.reserveDecimals(v/60, 2))}.sortByKey()
    val reactiveArr = reactiveByMonth.values.collect.mkString(",")
    outputStringList ::= "平均有效功率： " + activeMeanKw
    outputStringList ::= "有效功率中位数： " + activeMidKw
    outputStringList ::= "平均无效功率： " + reactiveMeanKW
    outputStringList ::= "平均用电量： " + meanKwh
    outputStringList ::= "总用电量： " + allKwh
    outputStringList ::= "按月累计：" + monthArr
    outputStringList ::= "按月累计有效功率：" + activeArr
    outputStringList ::= "按月累计无效功率：" + reactiveArr
    
    RedisDao.makePool()
    val redisClient = RedisDao.getPool.getResource()
    redisClient.publish("mllib_activeMeanKw", CommonUtil.reserveDecimals(activeMeanKw,3) +"")
    redisClient.publish("mllib_activeMidKw", CommonUtil.reserveDecimals(activeMidKw,3) +"")
    redisClient.publish("mllib_reactiveMeanKW", CommonUtil.reserveDecimals(reactiveMeanKW,3) +"")
    redisClient.publish("mllib_meanKwh", CommonUtil.reserveDecimals(meanKwh,3) +"")
    redisClient.publish("mllib_allKwh", CommonUtil.reserveDecimals(allKwh,3) +"")
    redisClient.publish("mllib_echart_months", monthArr)
    redisClient.publish("mllib_echart_actives", activeArr)
    redisClient.publish("mllib_echart_reactives", reactiveArr)
    
    //存入redis的JSONObject对象
    val redisJsonArray = new JSONArray()
    redisJsonArray.put(new JSONObject().put("mllib_activeMeanKw", CommonUtil.reserveDecimals(activeMeanKw,3) +""))
    redisJsonArray.put(new JSONObject().put("mllib_activeMidKw", CommonUtil.reserveDecimals(activeMidKw,3) +""))
    redisJsonArray.put(new JSONObject().put("mllib_reactiveMeanKW", CommonUtil.reserveDecimals(reactiveMeanKW,3) +""))
    redisJsonArray.put(new JSONObject().put("mllib_meanKwh", CommonUtil.reserveDecimals(meanKwh,3) +""))
    redisJsonArray.put(new JSONObject().put("mllib_allKwh", CommonUtil.reserveDecimals(allKwh,3) +""))
    redisJsonArray.put(new JSONObject().put("mllib_echart_months", monthArr))
    redisJsonArray.put(new JSONObject().put("mllib_echart_actives", activeArr))
    redisJsonArray.put(new JSONObject().put("mllib_echart_reactives", reactiveArr))
    
    redisClient.setnx("HouseholdAnalysis_"+startTime.replaceAll(":", "#")+"_"+endTime.replaceAll(":", "#"), redisJsonArray.toString())
    outputStringList ::= "jsonObject:" + redisJsonArray.toString()
    
    RedisDao.getPool.returnResource(redisClient)

    val end = System.currentTimeMillis()
    outputStringList ::= "time:" + (end - start).toDouble/1000
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