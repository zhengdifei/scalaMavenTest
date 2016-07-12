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
import org.apache.spark.mllib.tree.impurity.Gini

/**
 * 家庭空调预测测试
 */
object HouseholdClassificationTest{

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
      var la = 0.0
      if(hd(8).toFloat >= 16)
        la = 1.0
      ds(2).toDouble :: (ds(1).toDouble :: (ds(0).toDouble :: (ts(0).toDouble :: (ts(1).toDouble :: (la :: Nil)))))
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
    	val model = ClassificationUtil.getLogicModel(vdata,param._2.toInt,0.0,1.0,new SimpleUpdater)
    	val scoreAndLabels = vdata.map{ point => 
    		(model.predict(point.features),point.label)
	    }
	    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    	outputStringList ::= "iteration : " + param._1 + " , result : " + metrics.areaUnderROC
    	redisClient.publish("mllib_iteration_rmsle" + param._1 , metrics.areaUnderROC + "")
      }
    }else if(args1(0).toInt == 2){
      paramsMap.map{ param =>
    	val model = ClassificationUtil.getLogicModel(vdata,10,param._2 ,1.0,new SimpleUpdater)
    	val scoreAndLabels = vdata.map{ point => 
    		(model.predict(point.features),point.label)
	    }
	    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    	outputStringList ::= "rege : " + param._1 + " , result : " + metrics.areaUnderROC
    	redisClient.publish("mllib_rege_rmsle" + param._1 , metrics.areaUnderROC + "")
      }
    }else if(args1(0).toInt == 3){
      paramsMap.map{ param =>
    	val model = ClassificationUtil.getLogicModel(vdata,10,0.0,param._2,new SimpleUpdater)
    	val scoreAndLabels = vdata.map{ point => 
    		(model.predict(point.features),point.label)
	    }
	    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    	outputStringList ::= "step : " + param._1 + " , result : " + metrics.areaUnderROC
    	redisClient.publish("mllib_step_rmsle" + param._1 , metrics.areaUnderROC + "")
      }
    }else if(args1(0).toInt == 4){
      paramsMap.map{ param =>
    	val model = ClassificationUtil.getSvmModel(vdata,param._2.toInt,0.0,1.0,new SimpleUpdater)
    	val scoreAndLabels = vdata.map{ point => 
    		(model.predict(point.features),point.label)
	    }
	    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    	outputStringList ::= "step : " + param._1 + " , result : " + metrics.areaUnderROC
    	redisClient.publish("mllib_step_rmsle" + param._1 , metrics.areaUnderROC + "")
      }
    }else if(args1(0).toInt == 5){
      paramsMap.map{ param =>
    	val model = ClassificationUtil.getNaiveBayesModel(vdata,param._2)
    	val scoreAndLabels = vdata.map{ point => 
    		(model.predict(point.features),point.label)
	    }
	    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    	outputStringList ::= "lamda : " + param._1 + " , result : " + metrics.areaUnderROC
    	redisClient.publish("mllib_lamda_rmsle" + param._1 , metrics.areaUnderROC + "")
      }
    }else if(args1(0).toInt == 6){
       paramsMap.map{ param =>
       	val model = DecisionTree.train(vdata, Algo.Classification, Gini, param._2.toInt)
    	val scoreAndLabels = vdata.map{ point => 
    		(model.predict(point.features),point.label)
	    }
    	val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    	outputStringList ::= "depth : " + param._1 + " , result : " + metrics.areaUnderROC
    	redisClient.publish("mllib_depth_rmsle" + param._1 , metrics.areaUnderROC + "")
       }
    }else if(args1(0).toInt == 7){
	    val model1 = DecisionTree.train(vdata, Algo.Classification, Entropy, 10)
    	val scoreAndLabels1 = vdata.map{ point => 
    		(model1.predict(point.features),point.label)
	    }
    	val metrics1 = new BinaryClassificationMetrics(scoreAndLabels1)
    	outputStringList ::= "Entropy result : " + metrics1.areaUnderROC
    	redisClient.publish("mllib_entropy_rmsle" , metrics1.areaUnderROC + "")
    	
    	val model2 = DecisionTree.train(vdata, Algo.Classification, Gini, 10)
    	val scoreAndLabels2 = vdata.map{ point => 
    		(model2.predict(point.features),point.label)
	    }
    	val metrics2 = new BinaryClassificationMetrics(scoreAndLabels2)
    	outputStringList ::= "Gini result : " + metrics2.areaUnderROC
    	redisClient.publish("mllib_gini_rmsle" , metrics2.areaUnderROC + "")
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