package com.visenergy.powerAnalysis

import org.apache.spark.mllib.classification.ClassificationModel
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.Updater
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Gini
import org.apache.spark.mllib.tree.impurity.Impurity
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.numericRDDToDoubleRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object ClassificationUtil {
	/*
	 * 逻辑回归正确率
	 */
    def logicEvaluate(trainData:RDD[LabeledPoint],testData:RDD[LabeledPoint],total:Long=0,numIterations:Int=10):Tuple3[Double,Double,Double] = {
	    val lrModel = LogisticRegressionWithSGD.train(trainData, numIterations)
	    val scoreAndLabels = testData.map{ point => 
    		(lrModel.predict(point.features),point.label)
	    }
	    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
	    
	    
	    val lrTotalCorrect = scoreAndLabels.map{case(p,l) => 
	      if(p == l) 1 else 0
	    }.sum
	    
	    var numTotal:Long = total
	    if(numTotal == 0 ){
	      numTotal = testData.count
	    }
	    (lrTotalCorrect/numTotal,metrics.areaUnderPR,metrics.areaUnderROC)
    }
    
    /*
	 * 支持向量机正确率
	 */
    def svmEvaluate(trainData:RDD[LabeledPoint],testData:RDD[LabeledPoint],total:Long=0,numIterations:Int=10):Tuple3[Double,Double,Double] = {
	    val svmModel = SVMWithSGD.train(trainData, numIterations)
	    val scoreAndLabels = testData.map{ point => 
    		(svmModel.predict(point.features),point.label)
	    }
	    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
	    
	    val svmTotalCorrect = scoreAndLabels.map{case(p,l) => 
	      if(p == l) 1 else 0
	    }.sum
	    
	    var numTotal:Long = total
	    if(numTotal == 0 ){
	      numTotal = testData.count
	    }
	    (svmTotalCorrect/numTotal,metrics.areaUnderPR,metrics.areaUnderROC)
    }
    
    /*
	 * 朴素贝叶斯正确率
	 */
    def naiveBayesEvaluate(trainData:RDD[LabeledPoint],testData:RDD[LabeledPoint],total:Long=0,numIterations:Int=10):Tuple3[Double,Double,Double] = {
	    val nbModel = NaiveBayes.train(trainData)
	    val scoreAndLabels = testData.map{ point => 
    		(nbModel.predict(point.features),point.label)
	    }
	    val metrics = new BinaryClassificationMetrics(scoreAndLabels)

	    val nbTotalCorrect = scoreAndLabels.map{case(p,l) => 
	      if(p == l) 1 else 0
	    }.sum
	    var numTotal:Long = total
	    if(numTotal == 0 ){
	      numTotal = testData.count
	    }

	    (nbTotalCorrect/numTotal,metrics.areaUnderPR,metrics.areaUnderROC)
    }
    
    /*
	 * 决策树正确率
	 */
    def decisionTreeEvaluate(trainData:RDD[LabeledPoint],testData:RDD[LabeledPoint],total:Long=0,impurity:Impurity=Gini,maxDepth:Int=5):Tuple3[Double,Double,Double] = {
	    val dtModel = DecisionTree.train(trainData,Algo.Classification,impurity,maxDepth)
	    val scoreAndLabels = testData.map{ point => 
    		(dtModel.predict(point.features),point.label)
	    }
	    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
	    
	    val dtTotalCorrect = scoreAndLabels.map{case(p,l) => 
	      if(p == l) 1 else 0
	    }.sum
	    
	    var numTotal:Long = total
	    if(numTotal == 0 ){
	      numTotal = testData.count
	    }
	    
	    (dtTotalCorrect/numTotal,metrics.areaUnderPR,metrics.areaUnderROC)
    }
    
    /*
	 * 随机深林正确率
	 */
    def randomForestEvaluate(trainData:RDD[LabeledPoint],testData:RDD[LabeledPoint],total:Long=0,numClasses:Int,numTrees:Int=2,featureSubsetStrategy: String="auto",impurity:String="gini",maxDepth:Int=5,maxBins:Int=100):Tuple3[Double,Double,Double] = {
    	val rfModel = RandomForest.trainClassifier(trainData,numClasses,scala.collection.immutable.Map[Int, Int](),numTrees,featureSubsetStrategy,impurity,maxDepth,maxBins)
	    val scoreAndLabels = testData.map{ point => 
    		(rfModel.predict(point.features),point.label)
	    }
	    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
	    
    	val rfTotalCorrect = scoreAndLabels.map{case(p,l) => 
	      if(p == l) 1 else 0
	    }.sum
	    var numTotal:Long = total
	    if(numTotal == 0 ){
	      numTotal = testData.count
	    }
	    (rfTotalCorrect/numTotal,metrics.areaUnderPR,metrics.areaUnderROC)
    }
    
   /*
   * 获取逻辑分类模型
   */
  def getLogicModel(input : RDD[LabeledPoint],numIterations:Int,regParam:Double,stepSize:Double,updater:Updater):ClassificationModel = {
    val lr = new LogisticRegressionWithSGD
    lr.optimizer.setNumIterations(numIterations).setUpdater(updater).setRegParam(regParam).setStepSize(stepSize)
    lr.run(input)
  }
   /*
   * 获取SVM模型
   */
  def getSvmModel(input : RDD[LabeledPoint],numIterations:Int,regParam:Double,stepSize:Double,updater:Updater):ClassificationModel = {
    val svm = new SVMWithSGD
    svm.optimizer.setNumIterations(numIterations).setUpdater(updater).setRegParam(regParam).setStepSize(stepSize)
    svm.run(input)
  }
  /*
   * 获取朴素贝叶斯模型
   */
  def getNaiveBayesModel(input : RDD[LabeledPoint],lambda : Double):ClassificationModel = {
    val nb = new NaiveBayes
    nb.setLambda(lambda)
    nb.run(input)
  }
  /*
   * 初始化二元元数据级
   */
  def initBinaryVector(data : RDD[List[Double]],bvNum : Int) : List[scala.collection.Map[Double,Long]] = {
    //构建二元数组
    var mappings= List[scala.collection.Map[Double,Long]]()
    for(i <- 0 to bvNum - 1){
      mappings ::= data.map(a => a(bvNum -1 -i)).distinct.zipWithIndex.collectAsMap()
    }
    mappings
  }
  
  /*
   * 向量变换
   */
   def extractFeature(record : List[Double],labelNum :Int,mappings:List[scala.collection.Map[Double,Long]]) : Array[Double] = {
	    //二元数组总长度
	    var cat_len = 0
	    for(i <- 0 to mappings.length-1){
	      cat_len += mappings(i).size
	    }
	    
	    val cat_vec = Array.ofDim[Double](cat_len)
		var step = 0
		for(a <- 0 to 4){
		  val m = mappings(a)
		  val idx = m(record(a)).toInt
		  
		  cat_vec(idx + step) = 1
		  step += m.size
		}
		//添加剩余实数特征
	    val rlen = record.length
		if(rlen > labelNum){
		  var other_vec = new Array[Double](rlen - labelNum)
		  for(j <- labelNum to rlen-1){
		    other_vec(j-labelNum) = record(j)
		  }
		  return cat_vec ++ other_vec
		}else{
		  return cat_vec
		}
    }
   /*
   * 创建二元向量
   */
  def transformBinaryVector(data : RDD[List[Double]],bvNum : Int,labelNum : Int,initMappings:List[scala.collection.Map[Double,Long]]):RDD[LabeledPoint] = {
    var mappings:List[scala.collection.Map[Double,Long]] = Nil
    if(initMappings != null || initMappings.isEmpty){
      mappings = initMappings
    }else{
      mappings = initBinaryVector(data,bvNum)
    }
   
    data.map( record => LabeledPoint(record(labelNum-1),Vectors.dense(extractFeature(record,labelNum,mappings))))
  }
}