package com.visenergy.MLlib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object MovieClusterTest {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("MovieFeatureTest").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    
    val movies = sc.textFile("test/ml-100k/u.item")
    println(movies.first)
    
    val genres = sc.textFile("test/ml-100k/u.genre")
    genres.take(5).foreach(println)
    
    val genresMap = genres.filter(!_.isEmpty()).map(line => line.split("\\|")).map(array => (array(1),array(0))).collectAsMap()
    println(genresMap)
    
    val titlesAndGenres = movies.map(_.split("\\|")).map{ array =>
    	val genres = array.toSeq.slice(5, array.size)
    	val genresAssigned = genres.zipWithIndex.filter{
    	  case (g,idx) =>	g == "1"
    	}.map{ case(g,idx) =>
    	  genresMap(idx.toString)
    	}
    	(array(0).toInt,(array(1),genresAssigned))
    }
    
    println(titlesAndGenres.first)
  }

}