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
object HouseholdDistributeByMonth{

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
    var startTime:String = "2006-01-01"
    var endTime:String = "2010-12-31"
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
    //输出
    var outputStringList = List[String]()
    //日期转换
    def transformMonth(month:Int):String = {
      if(month <10){
        "0" + month
      }else{
        "" + month
      }
    }
    val args1 = "2008年01月"
    val year = args1.substring(0, 4)
    val month = args1.substring(5, 7)

    val kitchenByMonth = hdData4.filter{record => record(0) == year.toDouble && record(1) == month.toDouble}.map{record => (record(0).toInt+"年"+transformMonth(record(1).toInt)+"月"+transformMonth(record(2).toInt)+"日",record(9))}.reduceByKey(_+_).map{ case (n,v) => (n,CommonUtil.reserveDecimals(v/1000, 2))}.sortByKey()
    val dayArr = kitchenByMonth.keys.collect.mkString(",")
    val daykitchenArr = kitchenByMonth.values.collect.mkString(",")
    val laundryByMonth = hdData4.filter{record => record(0) == year.toDouble && record(1) == month.toDouble}.map{record => (record(0).toInt+"年"+transformMonth(record(1).toInt)+"月"+transformMonth(record(2).toInt)+"日",record(10))}.reduceByKey(_+_).map{ case (n,v) => (n,CommonUtil.reserveDecimals(v/1000, 2))}.sortByKey()
    val daylaundryArr = laundryByMonth.values.collect.mkString(",")
    val bedroomByMonth = hdData4.filter{record => record(0) == year.toDouble && record(1) == month.toDouble}.map{record => (record(0).toInt+"年"+transformMonth(record(1).toInt)+"月"+transformMonth(record(2).toInt)+"日",record(11))}.reduceByKey(_+_).map{ case (n,v) => (n,CommonUtil.reserveDecimals(v/1000, 2))}.sortByKey()
    val daybedroomArr = bedroomByMonth.values.collect.mkString(",")
    
    outputStringList ::= "按日累计：" + dayArr
    outputStringList ::= "按日累计厨房：" + daykitchenArr
    outputStringList ::= "按日累计洗衣房：" + daylaundryArr
    outputStringList ::= "按日累计卧室：" + daybedroomArr
    
    RedisDao.makePool()
    val redisClient = RedisDao.getPool.getResource()
    redisClient.publish("mllib_echart_days", dayArr)
    redisClient.publish("mllib_echart_dayKitchens", daykitchenArr)
    redisClient.publish("mllib_echart_dayLaundrys", daylaundryArr)
    redisClient.publish("mllib_echart_dayBedrooms", daybedroomArr)
    
     //存入redis的JSONObject对象
    val redisJsonArray = new JSONArray()
    redisJsonArray.put(new JSONObject().put("mllib_echart_days",dayArr))
    redisJsonArray.put(new JSONObject().put("mllib_echart_dayKitchens", daykitchenArr))
    redisJsonArray.put(new JSONObject().put("mllib_echart_dayLaundrys", daylaundryArr))
    redisJsonArray.put(new JSONObject().put("mllib_echart_dayBedrooms", daybedroomArr))
    
    redisClient.setnx("HouseholdDistributeByMonth_"+args1, redisJsonArray.toString())
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