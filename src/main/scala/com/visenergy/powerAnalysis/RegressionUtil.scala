package com.visenergy.powerAnalysis

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.impurity.Entropy
import org.apache.spark.mllib.tree.impurity.Impurity
import org.apache.spark.mllib.tree.impurity.Gini
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.classification.ClassificationModel
import org.apache.spark.mllib.optimization.Updater
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LinearRegressionModel

object RegressionUtil {
  
  /*
   * 线性评估
   */
  def logicEvaluate(trainData:RDD[LabeledPoint],testData:RDD[LabeledPoint],numIterations:Int=10,step:Double=0.1):Tuple3[Double,Double,Double] = {
	  val lrModel = LinearRegressionWithSGD.train(trainData, numIterations,step)
	  val lrPredictData = testData.map{ point =>
	  	(point.label , lrModel.predict(point.features))
	  }
	  evaluateResult(lrPredictData)
  }
  /*
   * 决策树评估
   */
  def decisionTreeEvaluate(trainData:RDD[LabeledPoint],testData:RDD[LabeledPoint],impurity: String="variance",maxDepth: Int=30,maxBins: Int=100):Tuple3[Double,Double,Double] = {
      val dtModel = DecisionTree.trainRegressor(trainData,scala.collection.immutable.Map[Int, Int](),impurity,maxDepth,maxBins)
      val dtPredictData = testData.map{ point =>
      	(point.label,dtModel.predict(point.features))
      }
	  evaluateResult(dtPredictData)
  }
  /*
   * 评估计算
   */
  def evaluateResult(predictData:RDD[Tuple2[Double,Double]]):Tuple3[Double,Double,Double] = {
      val mse = predictData.map{case (t,p) => squared_error(t,p)}.mean()
	  val mae = predictData.map{case (t,p) => abs_error(t,p)}.mean()
      val rmsle = math.sqrt(predictData.map{case (t,p) => squared_log_error(t,p)}.mean())
      (mse,mae,rmsle)
  }
  /*
   * 获取逻辑分类模型
   */
  def getLogicModel(input : RDD[LabeledPoint],numIterations:Int,regParam:Double,stepSize:Double,updater:Updater):LinearRegressionModel = {
    val lr = new LinearRegressionWithSGD
    lr.optimizer.setNumIterations(numIterations).setUpdater(updater).setRegParam(regParam).setStepSize(stepSize)
    lr.run(input)
  }
  /*
   * 求MSE
   */
  def squared_error(actual:Double,pred:Double):Double = {
    math.pow(actual-pred,2)
  }
  /*
   * 求ASE
   */
  def abs_error(actual:Double,pred:Double) : Double = {
    math.abs(actual - pred)
  }
  /*
   * 求RMSLE
   */
  def squared_log_error(actual:Double,pred:Double) : Double = {
     math.pow(math.log(pred + 1) - math.log(actual + 1),2)
  }
}