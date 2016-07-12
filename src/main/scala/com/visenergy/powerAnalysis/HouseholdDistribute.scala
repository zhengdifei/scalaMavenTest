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
 * 家庭用电分布
 */
object HouseholdDistribute{

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
    var startTime:String = "2008-01-01"
    var endTime:String = "2008-12-31"
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
    
    //输出
    var outputStringList = List[String]()
    
    val allDayNum = hdData4.map(record => (record(0).toInt + "-" + record(1).toInt + "-" + record(2).toInt,1)).distinct.count
    val kitchenHourNum = hdData4.map(record => record(9)).filter(num => num != 0.0).count/60
    val laundryHourNum = hdData4.map(record => record(10)).filter(num => num != 0.0).count/60
    val bedroomHourNum = hdData4.map(record => record(11)).filter(num => num != 0.0).count/60
    
    val kitchenEnergyNum = hdData4.map(record => record(9)).sum/1000
    val laundryEnergyNum = hdData4.map(record => record(10)).sum/1000
    val bedroomEnergyNum = hdData4.map(record => record(11)).sum/1000
 
    //将月份1变成01格式
    def transformMonth(month:Int):String = {
      if(month <10){
        "0" + month
      }else{
        "" + month
      }
    }
    val kitchenByMonth = hdData4.map{record => (record(0).toInt+"年"+transformMonth(record(1).toInt)+"月",record(9))}.reduceByKey(_+_).map{ case (n,v) => (n,CommonUtil.reserveDecimals(v/1000, 2))}.sortByKey()
    val monthArr = kitchenByMonth.keys.collect.mkString(",")
    val kitchenArr = kitchenByMonth.values.collect.mkString(",")
    val laundryByMonth = hdData4.map{record => (record(0).toInt+"年"+transformMonth(record(1).toInt)+"月",record(10))}.reduceByKey(_+_).map{ case (n,v) => (n,CommonUtil.reserveDecimals(v/1000, 2))}.sortByKey()
    val laundryArr = laundryByMonth.values.collect.mkString(",")
    val bedroomByMonth = hdData4.map{record => (record(0).toInt+"年"+transformMonth(record(1).toInt)+"月",record(11))}.reduceByKey(_+_).map{ case (n,v) => (n,CommonUtil.reserveDecimals(v/1000, 2))}.sortByKey()
    val bedroomArr = bedroomByMonth.values.collect.mkString(",")
    
    outputStringList ::= "总天数： " + allDayNum
    outputStringList ::= "厨房总时间： " + kitchenHourNum
    outputStringList ::= "洗衣房总时间： " + laundryHourNum
    outputStringList ::= "卧室总时间： " + bedroomHourNum
    outputStringList ::= "厨房平均每天使用时间： " + kitchenHourNum.toDouble/(allDayNum)
    outputStringList ::= "洗衣房平均每天使用时间： " + laundryHourNum.toDouble/(allDayNum)
    outputStringList ::= "卧室平均每天使用时间：" + bedroomHourNum.toDouble/(allDayNum)

    outputStringList ::= "厨房总用电量： " + kitchenEnergyNum
    outputStringList ::= "洗衣房总用电量： " + laundryEnergyNum
    outputStringList ::= "卧室总用电量： " + bedroomEnergyNum
    outputStringList ::= "厨房平均每天用电量： " + kitchenEnergyNum.toDouble/allDayNum
    outputStringList ::= "洗衣房平均每天用电量： " + laundryEnergyNum.toDouble/allDayNum
    outputStringList ::= "卧室平均每天用电量：" + bedroomEnergyNum.toDouble/allDayNum
    
    RedisDao.makePool()
    val redisClient = RedisDao.getPool.getResource()
    redisClient.publish("mllib_kitchenHourNum", kitchenHourNum +"")
    redisClient.publish("mllib_kitchenHourNumByDay", CommonUtil.reserveDecimals(kitchenHourNum.toDouble/allDayNum,2) +"")
    redisClient.publish("mllib_laundryHourNum", laundryHourNum +"")
    redisClient.publish("mllib_laundryHourNumByDay", CommonUtil.reserveDecimals(laundryHourNum.toDouble/allDayNum,2) +"")
    redisClient.publish("mllib_bedroomHourNum", bedroomHourNum +"")
    redisClient.publish("mllib_bedroomHourNumByDay", CommonUtil.reserveDecimals(bedroomHourNum.toDouble/allDayNum,2) +"")
    redisClient.publish("mllib_kitchenEnergyNum", kitchenEnergyNum +"")
    redisClient.publish("mllib_kitchenEnergyNumByDay", CommonUtil.reserveDecimals(kitchenEnergyNum.toDouble/allDayNum,2) +"")
    redisClient.publish("mllib_laundryEnergyNum", laundryEnergyNum +"")
    redisClient.publish("mllib_laundryEnergyNumByDay", CommonUtil.reserveDecimals(laundryEnergyNum.toDouble/allDayNum,2) +"")
    redisClient.publish("mllib_bedroomEnergyNum", bedroomEnergyNum +"")
    redisClient.publish("mllib_bedroomEnergyNumByDay", CommonUtil.reserveDecimals(bedroomEnergyNum.toDouble/allDayNum,2) +"")
    
    redisClient.publish("mllib_echart_months", monthArr)
    redisClient.publish("mllib_echart_kitchens", kitchenArr)
    redisClient.publish("mllib_echart_laundrys", laundryArr)
    redisClient.publish("mllib_echart_bedrooms", bedroomArr)
    
    //存入redis的JSONObject对象
    val redisJsonArray = new JSONArray()
    redisJsonArray.put(new JSONObject().put("mllib_kitchenHourNum", kitchenHourNum +""))
    redisJsonArray.put(new JSONObject().put("mllib_kitchenHourNumByDay", CommonUtil.reserveDecimals(kitchenHourNum.toDouble/allDayNum,2) +""))
    redisJsonArray.put(new JSONObject().put("mllib_laundryHourNum", laundryHourNum +""))
    redisJsonArray.put(new JSONObject().put("mllib_laundryHourNumByDay", CommonUtil.reserveDecimals(laundryHourNum.toDouble/allDayNum,2) +""))
    redisJsonArray.put(new JSONObject().put("mllib_bedroomHourNum", bedroomHourNum +""))
    redisJsonArray.put(new JSONObject().put("mllib_bedroomHourNumByDay", CommonUtil.reserveDecimals(bedroomHourNum.toDouble/allDayNum,2) +""))
    redisJsonArray.put(new JSONObject().put("mllib_kitchenEnergyNum", kitchenEnergyNum +""))
    redisJsonArray.put(new JSONObject().put("mllib_kitchenEnergyNumByDay", CommonUtil.reserveDecimals(kitchenEnergyNum.toDouble/allDayNum,2) +""))
    redisJsonArray.put(new JSONObject().put("mllib_laundryEnergyNum", laundryEnergyNum +""))
    redisJsonArray.put(new JSONObject().put("mllib_laundryEnergyNumByDay", CommonUtil.reserveDecimals(laundryEnergyNum.toDouble/allDayNum,2) +""))
    redisJsonArray.put(new JSONObject().put("mllib_bedroomEnergyNum", bedroomEnergyNum +""))
    redisJsonArray.put(new JSONObject().put("mllib_bedroomEnergyNumByDay", CommonUtil.reserveDecimals(bedroomEnergyNum.toDouble/allDayNum,2) +""))
    
    redisJsonArray.put(new JSONObject().put("mllib_echart_months", monthArr))
    redisJsonArray.put(new JSONObject().put("mllib_echart_kitchens", kitchenArr))
    redisJsonArray.put(new JSONObject().put("mllib_echart_laundrys", laundryArr))
    redisJsonArray.put(new JSONObject().put("mllib_echart_bedrooms", bedroomArr))
    
    redisClient.setnx("HouseholdDistribute_"+startTime.replaceAll(":", "#")+"_"+endTime.replaceAll(":", "#"), redisJsonArray.toString())
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