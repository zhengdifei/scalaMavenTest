package com.visenergy.MLlib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
/**
 * 验证推荐内容
 */
object MovieFeatureTest2 {

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
  }
}