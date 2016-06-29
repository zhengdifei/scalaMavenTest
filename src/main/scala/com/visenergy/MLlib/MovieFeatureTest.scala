package com.visenergy.MLlib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
/**
 * 用户推荐
 */
object MovieFeatureTest {

  def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setAppName("MLlibTest").setMaster("local")
    	val sc = new SparkContext(sparkConf)
		
		val rawData = sc.textFile("test/ml-100k/u.data")
		println(rawData.first())
		
		val rawRatings = rawData.map(_.split("\t").take(3))
		println(rawRatings.first())
		
		val ratings = rawRatings.map{
		  case Array(user,movie,rating) => 
		    Rating(user.toInt,movie.toInt,rating.toDouble)
		}
		println(ratings.first())
		
		val model = ALS.train(ratings, 50,10, 0.01)
		
		println(model.userFeatures.count)
		println(model.productFeatures.count)
		
		val predictRating = model.predict(789, 123)
		println(predictRating)
		
		val topKRecs = model.recommendProducts(789, 10)
		println(topKRecs.mkString("\n"))
  }
}