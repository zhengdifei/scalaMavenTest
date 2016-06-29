package com.visenergy.MLlib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.mllib.evaluation.RegressionMetrics
/**
 * 均方差评估推荐内容
 */
object MovieFeatureTest4 {

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
		
		//MSE,RMSE评估
		val actualRating = moviesForUser.take(1)(0)
		println(actualRating)
		val predictedRating = model.predict(789, actualRating.product)
		println(predictedRating)
		val squaredError = math.pow(predictedRating - actualRating.rating,2.0)
		println(squaredError)
		
		val userProducts = ratings.map{
		  case Rating(user,product,rating) => (user,product)
		}
		
		val predictions = model.predict(userProducts).map{
		  case Rating(user,product,rating) =>((user,product),rating)
		}
		
		val ratingsAndPredictions = ratings.map{
		  case Rating(user,product,rating) => ((user,product),rating)
		}.join(predictions)
		
		val MSE = ratingsAndPredictions.map{
		  case ((user,product),(actual,predicted)) => 
		    math.pow((actual-predicted), 2)
		}.reduce(_ + _)/ratingsAndPredictions.count()
		
		println("Mean Sequared Error = " + MSE)
		
		val RMSE = math.sqrt(MSE)
		println("Root Mean Squared Error = " + RMSE)
		
		//RegressionMetrics
		val predictedAndTrue = ratingsAndPredictions.map{
		  case ((user,product),(predicted,actual)) => (predicted,actual)
		}
		
		val regressionMetrics = new RegressionMetrics(predictedAndTrue)
		println("Mean Squared Error = " + regressionMetrics.meanSquaredError)
		println("Root Mean Squared Error = " + regressionMetrics.rootMeanSquaredError)
  }
}