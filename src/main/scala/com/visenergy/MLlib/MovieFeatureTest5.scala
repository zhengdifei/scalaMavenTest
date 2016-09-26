package com.visenergy.MLlib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.jblas.DoubleMatrix
import org.apache.spark.mllib.evaluation.RankingMetrics

/**
 * K值平均准确率评估推荐内容
 */
object MovieFeatureTest5 {

  def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setAppName("MLlibTest2").setMaster("local")
    	val sc = new SparkContext(sparkConf)
		
		val rawData = sc.textFile("test/ml-100k/u.data")
		println(rawData.first())
		
		val rawRatings = rawData.map(_.split("\t").take(3))
		println(rawRatings.first())
		
		val ratings = rawRatings.map{
		  case Array(user,movie,rating) => 
		    Rating(user.toInt,movie.toInt,rating.toDouble)
		}
		
		val movies = sc.textFile("test/ml-100k/u.item")
		val titles = movies.map(line => line.split("\\|").take(2)).map(array => (array(0).toInt,array(1))).collectAsMap()
		println(titles(123))
		
		val moviesForUser = ratings.keyBy(_.user).lookup(789)
		println(moviesForUser.size)
		
		moviesForUser.sortBy(-_.rating ).take(10).map(rating => (titles(rating.product ),rating.rating )).foreach(println)
		
		val model = ALS.train(ratings, 50,10, 0.01)
		val topKRecs = model.recommendProducts(789, 10)
		topKRecs.map(rating => (titles(rating.product ),rating.rating)).foreach(println)
		
		//APK
		def avgPrecisionK(actual : Seq[Int],predicted : Seq[Int],k:Int) : Double = {
		  val predK = predicted.take(k)
		  var score = 0.0
		  var numHits = 0.0
		  for ((p,i) <- predK.zipWithIndex) {
		    if(actual.contains(p)){
		      numHits += 1.0
		      score += numHits / (i.toDouble + 1.0)
		    }
		  }
		  if(actual.isEmpty) {
		    1.0
		  } else {
		    score / math.min(actual.size, k).toDouble
		  }
		}
		
		val actualMovies = moviesForUser.map(_.product )
		val predictedMovies = topKRecs.map(_.product )
		val apk10 = avgPrecisionK(actualMovies,predictedMovies,10)
		println(apk10)
		
		//MAPK
		val itemFactors = model.productFeatures.map{
		  case (id,factor) => factor
		}.collect()
		
		val itemMatrix = new DoubleMatrix(itemFactors)
		println(itemMatrix.rows,itemMatrix.columns)
		
		val imBroadcast = sc.broadcast(itemMatrix)
		val allRecs = model.userFeatures.map{
		  case (userId,array) => 
		    val userVector = new DoubleMatrix(array)
		    val scores = imBroadcast.value.mmul(userVector)
		    val sortedWithId = scores.data.zipWithIndex.sortBy(-_._1)
		    val recommendIds = sortedWithId.map(_._2 + 1).toSeq
		    (userId,recommendIds)
		}
		val userMovies = ratings.map{
		  case Rating(user,product,rating) =>
		    (user,product)
		}.groupBy(_._1)
		
		val MAPK = allRecs.join(userMovies).map{
		  case(userId,(predicted,actualWithIds)) =>
		    val actual = actualWithIds.map(_._2).toSeq
		    avgPrecisionK(actual,predicted,10)
		}.reduce(_+_) / allRecs.count
		
		println("Mean Average Precision at K = " + MAPK)
		
		//RankingMetrics
		val predictedAndTrueForRanking = allRecs.join(userMovies).map{
		  case(userId,(predicted,actualWithIds)) => 
		    val actual = actualWithIds.map(_._2)
		    (predicted.toArray,actual.toArray)
		}
		val rankingMetrics = new RankingMetrics(predictedAndTrueForRanking)
		println("Mean Average Precision = " + rankingMetrics.meanAveragePrecision)
		
		val MAPK2000 = allRecs.join(userMovies).map{
		  case(userId,(predicted,actualWithIds)) =>
		    val actual = actualWithIds.map(_._2).toSeq
		    avgPrecisionK(actual,predicted,2000)
		}.reduce(_+_) / allRecs.count
		println("Mean Average Precision = " + MAPK2000)

		
  }
}