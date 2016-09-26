package com.visenergy.MLlib

import scala.collection.Map
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.Updater
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.DecisionTree
/**
 * 回归：两种回归的方法
 */
object HouseholdRegressionTest {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HouseholdTest").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    
    val start = System.currentTimeMillis()
    
    val hdData = sc.textFile("test/household/household1.txt")
    
    val hdData2 = hdData.filter(d => !d.contains("?")).map(d => d.split(";"))
    //println(hdData2.first())
    
    val hdData3 = hdData2.map{ hd =>
      val ds = hd(0).split("/")
      val ts = hd(1).split(":")
      var la = hd(2).toDouble
//      if(hd(8).toFloat >= 16)
//        la = 1
      
      (ds(2).toInt,ds(1).toInt,ds(0).toInt,ts(0).toInt,ts(1).toInt,la)

//      (ds(2).toInt,ds(1).toInt,ds(0).toInt,ts(0).toInt,ts(1).toInt,la,hd(8).toDouble,hd(3).toDouble,hd(4).toDouble,hd(5).toDouble,hd(6).toDouble,hd(7).toDouble)
    }
    
    var mappings= List[Map[Int,Long]]()
      
    mappings ::= hdData3.map(a => a._5).distinct.zipWithIndex.collectAsMap()
    mappings ::= hdData3.map(a => a._4).distinct.zipWithIndex.collectAsMap()
    mappings ::= hdData3.map(a => a._3).distinct.zipWithIndex.collectAsMap()
    mappings ::= hdData3.map(a => a._2).distinct.zipWithIndex.collectAsMap()
    mappings ::= hdData3.map(a => a._1).distinct.zipWithIndex.collectAsMap()
    
    //总长度
    var cat_len = 0
    for(i <- 0 to mappings.length-1){
      cat_len += mappings(i).size
    }

//    def extract_feature(record:Tuple12[Int,Int,Int,Int,Int,Double,Double,Double,Double,Double,Double,Double]) = {

    def extract_feature(record:Tuple6[Int,Int,Int,Int,Int,Double]) = {
//    	val cat_vec = new Array[Double](cat_len)
//    	for(i <- 0 to cat_len-1){
//    	  cat_vec(i) = 0
//    	}
        val cat_vec = Array.ofDim[Double](cat_len)
    	var step = 0
    	for(a <- 0 to 4){
    	  val m = mappings(a)
    	  var idx = 0
    	  if(a == 0) idx = m(record._1).toInt
    	  else if(a == 1) idx = m(record._2).toInt
    	  else if(a == 2) idx = m(record._3).toInt
    	  else if(a == 3) idx = m(record._4).toInt
    	  else if(a == 4) idx = m(record._5).toInt
    	  
    	  cat_vec(idx + step) = 1
    	  step += m.size
    	}
        
        cat_vec
//        val other_vec = Array[Double](record._7,record._8,record._9,record._10,record._11,record._12)
//    	cat_vec ++ other_vec
    }
    //输出
    var outputStringList = List[String]()
    
    val vdata = hdData3.map( record => LabeledPoint(record._6,Vectors.dense(extract_feature(record))))
    vdata.cache
//    val first_vdata = vdata.first
//    outputStringList ::= "label:" + first_vdata.label
//    outputStringList ::= "feature:" + first_vdata.features
    /*
     * 线性回归
     */
//    val lrModel = LinearRegressionWithSGD.train(vdata, 10,0.1)
//    val lrPredictData = vdata.map{ point =>
//      (point.label , lrModel.predict(point.features))
//    }
    //lrPredictData.take(5).foreach(println)
    /*
     * 性能评估(c8)
     * Linear Model MSE : 21271380977120896000000000000000000000000000000000000000000000000000000.0000
     * Linear Model MAE : 145834299847179110000000000000000000.0000
     * Linear Model RMSLE : NaN
     * 
     * 性能评估(c2)
     * Linear Model MSE : 616417752670453000000000000000000000000000000000000000000000000000000.0000
     * Linear Model MAE : 24825647984091216000000000000000000.0000
     * Linear Model RMSLE : NaN
     */
//    val lr_mse = lrPredictData.map{case (t,p) => squared_error(t,p)}.mean()
//    val lr_mae = lrPredictData.map{case (t,p) => abs_error(t,p)}.mean()
//    val lr_rmsle = math.sqrt(lrPredictData.map{case (t,p) => squared_log_error(t,p)}.mean())
//    outputStringList ::= f"Linear Model MSE : ${lr_mse}%2.4f"
//    outputStringList ::= f"Linear Model MAE : ${lr_mae}%2.4f"
//    outputStringList ::= f"Linear Model RMSLE : ${lr_rmsle}%2.4f"
    /*
     * 决策树回归
     */
    val dtModel = DecisionTree.trainRegressor(vdata,scala.collection.immutable.Map[Int, Int](),"variance",20,100)
    val dtPredictData = vdata.map{ point =>
      val pvalue = dtModel.predict(point.features)
      println(point.label +"   :   "+ pvalue)
      (point.label,pvalue)
    }
    //dtPredictData.take(5).foreach(println)
    /*
     * 性能评估(c8)
     * DecisionTree Model MSE : 22.6684
     * DecisionTree Model MAE : 2.6918
     * DecisionTree Model RMSLE : 0.8169
     * 
     * 性能评估(c2)
     * DecisionTree Model MSE : 0.0298
     * DecisionTree Model MAE : 0.1182
     * DecisionTree Model RMSLE : 0.0674
     */
    val dt_mse = dtPredictData.map{case (t,p) => squared_error(t,p)}.mean()
    val dt_mae = dtPredictData.map{case (t,p) => abs_error(t,p)}.mean()
    val dt_rmsle = math.sqrt(dtPredictData.map{case (t,p) => squared_log_error(t,p)}.mean())
    outputStringList ::= f"DecisionTree Model MSE : ${dt_mse}%2.4f"
    outputStringList ::= f"DecisionTree Model MAE : ${dt_mae}%2.4f"
    outputStringList ::= f"DecisionTree Model RMSLE : ${dt_rmsle}%2.4f"
    
    val end = System.currentTimeMillis()
    outputStringList ::= f"time: ${(end - start)/1000}%5.2f"
    //逆序
    //outputStringList.foreach(println)
    //正序输出
    var n = outputStringList.length - 1
    while(n>=0){
      println(outputStringList(n))
      n = n -1 
    }
  }
  
  def squared_error(actual:Double,pred:Double):Double = {
    math.pow(actual-pred,2)
  }
  
  def abs_error(actual:Double,pred:Double) : Double = {
    math.abs(actual - pred)
  }
  
  def squared_log_error(actual:Double,pred:Double) : Double = {
     math.pow(math.log(pred + 1) - math.log(actual + 1),2)
  }
}