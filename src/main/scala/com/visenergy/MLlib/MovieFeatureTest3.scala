package com.visenergy.MLlib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.jblas.DoubleMatrix
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS

/**
 * 物品推荐
 */
object MovieFeatureTest3 {

  def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setAppName("MLlibTest3").setMaster("local")
    	val sc = new SparkContext(sparkConf)
		
		val rawData = sc.textFile("test/ml-100k/u.data")
		val rawRatings = rawData.map(_.split("\t").take(3))
		val ratings = rawRatings.map{
		  case Array(user,movie,rating) => 
		    Rating(user.toInt,movie.toInt,rating.toDouble)
		}
		
		val movies = sc.textFile("test/ml-100k/u.item")
		val titles = movies.map(line => line.split("\\|").take(2)).map(array => (array(0).toInt,array(1))).collectAsMap()
		
		
		val model = ALS.train(ratings, 50,10, 0.01)
		
		val aMatrix = new DoubleMatrix(Array(1.0,2.0,3.0))
		
		def cosineSimilarity(vec1 : DoubleMatrix,vec2:DoubleMatrix) : Double = {
		  vec1.dot(vec2) /(vec1.norm2() * vec2.norm2())
		}
		
		val itemFactor = model.productFeatures .lookup(567).head
		val itemVector = new DoubleMatrix(itemFactor)
		val pro1 = cosineSimilarity(itemVector,itemVector)
		println(pro1)
		
		val sims = model.productFeatures.map{ case (id,factor) =>
		  val factorVector = new DoubleMatrix(factor)
		  val sim = cosineSimilarity(factorVector,itemVector)
		  (id,sim)
		}
		
		val sortedSims = sims.top(10)(Ordering.by[(Int,Double),Double]{case
		  (id,similarity) => similarity
		})
		
		println(sortedSims.take(10).mkString("\n"))
		
		val sortedSims2 = sims.top(11)(Ordering.by[(Int,Double),Double]{case
		  (id,similarity) => similarity
		})
		
		println(sortedSims2.slice(1, 11).map{ case (id,sim) => (titles(id),sim)}.mkString("\n"))
  }
}