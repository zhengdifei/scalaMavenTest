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
import org.apache.spark.mllib.optimization.SimpleUpdater

/**
 * 家庭用电预测测试
 */
object HouseholdRegressionTest{

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
    val testStart = System.currentTimeMillis()
    redisClient.publish("mllib_testInit_time", (testStart - start).toDouble/1000 + " s")
    //线性回归
    //val args1:Array[String] = Array("1","1,5,10,20,50")
    //val args1:Array[String] = Array("2","0.001,0.01,0.1,1.0,10.0")
    val args1:Array[String] = Array("3","0.001,0.01,0.1,1.0,10.0")
    val arr = args1(1).split(",")
	val paramsMap:scala.collection.mutable.Map[Int,Double] = scala.collection.mutable.Map[Int,Double]()
	for(i <- 0 to arr.length-1){
		paramsMap += ((i+1) -> arr(i).toDouble)
	}
    
    if(args1(0).toInt == 1){
      paramsMap.map{ param =>
    	val model = RegressionUtil.getLogicModel(vdata,param._2.toInt,0.0,1.0,new SimpleUpdater)
    	val lrPredictData = vdata.map{ point =>
	  		(point.label , model.predict(point.features))
    	}
    	val result = RegressionUtil.evaluateResult(lrPredictData)
    	outputStringList ::= "iteration : " + param._1 + " , result : " + result._3
    	redisClient.publish("mllib_iteration_rmsle" + param._1 , result._3 + "")
      }
    }else if(args1(0).toInt == 2){
      paramsMap.map{ param =>
    	val model = RegressionUtil.getLogicModel(vdata,10,param._2 ,1.0,new SimpleUpdater)
    	val lrPredictData = vdata.map{ point =>
	  		(point.label , model.predict(point.features))
    	}
    	val result = RegressionUtil.evaluateResult(lrPredictData)
    	outputStringList ::= "rege : " + param._1 + " , result : " + result._3
    	redisClient.publish("mllib_rege_rmsle" + param._1 , result._3 + "")
      }
    }else if(args1(0).toInt == 3){
      paramsMap.map{ param =>
    	val model = RegressionUtil.getLogicModel(vdata,10,0.0,param._2,new SimpleUpdater)
    	val lrPredictData = vdata.map{ point =>
	  		(point.label , model.predict(point.features))
    	}
    	val result = RegressionUtil.evaluateResult(lrPredictData)
    	outputStringList ::= "step : " + param._1 + " , result : " + result._3
    	redisClient.publish("mllib_step_rmsle" + param._1 , result._3 + "")
      }
    }else if(args1(0).toInt == 4){
       paramsMap.map{ param =>
       	val model = DecisionTree.trainRegressor(vdata,scala.collection.immutable.Map[Int, Int](),"variance",param._2.toInt,100)
    	val dtPredictData = vdata.map{ point =>
	  		(point.label , model.predict(point.features))
    	}
    	val result = RegressionUtil.evaluateResult(dtPredictData)
    	outputStringList ::= "step : " + param._1 + " , result : " + result._3
    	redisClient.publish("mllib_depth_rmsle" + param._1 , result._3 + "")
       }
    }else if(args1(0).toInt == 5){
        paramsMap.map{ param =>
	       	val model = DecisionTree.trainRegressor(vdata,scala.collection.immutable.Map[Int, Int](),"variance",10,param._2.toInt)
	    	val dtPredictData = vdata.map{ point =>
		  		(point.label , model.predict(point.features))
	    	}
	    	val result = RegressionUtil.evaluateResult(dtPredictData)
	    	outputStringList ::= "step : " + param._1 + " , result : " + result._3
	    	redisClient.publish("mllib_bins_rmsle" + param._1 , result._3 + "")
       }
    }else{
      outputStringList ::= "错误参数"
    }

    //决策树
    val testEnd = System.currentTimeMillis()
    redisClient.publish("mllib_testEnd_time", (testEnd - testStart).toDouble/1000 + " s")
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