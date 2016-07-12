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
object HouseholdAnalysisByMonth{

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
    
    //数据ETL
    val hdData2 = hdData.filter(d => !d.contains("?")).map(d => d.split(";"))
    //转换
    val hdData4 = hdData2.map{ hd =>
      val ds = hd(0).split("/")
      val ts = hd(1).split(":")
      
      ds(2).toDouble :: (ds(1).toDouble :: (ds(0).toDouble :: (ts(0).toDouble :: (ts(1).toDouble :: (hd(2).toDouble :: (hd(3).toDouble :: (hd(4).toDouble :: (hd(5).toDouble ::(hd(6).toDouble :: (hd(7).toDouble :: (hd(8).toDouble :: Nil)))))))))))
    }
    //输出
    var outputStringList = List[String]()
    //将月份1变换成01
    def transformMonth(month:Int):String = {
      if(month <10){
        "0" + month
      }else{
        "" + month
      }
    }
    val args1 = "2007年07月"
    val year = args1.substring(0, 4)
    val month = args1.substring(5, 7)
    val activeByMonth = hdData4.filter{record => record(0) == year.toDouble && record(1) == month.toDouble}.map{record => (record(2).toInt,record(5))}.reduceByKey(_+_).map{ case (n,v) => (n,CommonUtil.reserveDecimals(v/60, 2))}.sortByKey()
    val dayArr = activeByMonth.keys.collect.mkString(",")
    val dayActiveArr = activeByMonth.values.collect.mkString(",")
    val reactiveByMonth = hdData4.filter{record => record(0) == year.toDouble && record(1) == month.toDouble}.map{record => (record(2).toInt,record(6))}.reduceByKey(_+_).map{ case (n,v) => (n,CommonUtil.reserveDecimals(v/60, 2))}.sortByKey()
    val dayReactiveArr = reactiveByMonth.values.collect.mkString(",")

    outputStringList ::= "按日累计：" + dayArr
    outputStringList ::= "按日累计有效功率：" + dayActiveArr
    outputStringList ::= "按日累计无效功率：" + dayReactiveArr
    
    RedisDao.makePool()
    val redisClient = RedisDao.getPool.getResource()
    //redis发布信息
    redisClient.publish("mllib_echart_days", dayArr)
    redisClient.publish("mllib_echart_dayActiveName", args1)
    redisClient.publish("mllib_echart_dayActives", dayActiveArr)
    redisClient.publish("mllib_echart_dayReactives", dayReactiveArr)
    
    //存入redis的JSONObject对象
    //存入redis的JSONObject对象
    val redisJsonArray = new JSONArray()
    redisJsonArray.put(new JSONObject().put("mllib_echart_days", dayArr))
    redisJsonArray.put(new JSONObject().put("mllib_echart_dayActiveName", args1))
    redisJsonArray.put(new JSONObject().put("mllib_echart_dayActives", dayActiveArr))
    redisJsonArray.put(new JSONObject().put("mllib_echart_dayReactives", dayReactiveArr))
    
    redisClient.setnx("HouseholdAnalysisByMonth_"+args1, redisJsonArray.toString())
    outputStringList ::= "jsonObject:" + redisJsonArray.toString()
    
    RedisDao.getPool.returnResource(redisClient)

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