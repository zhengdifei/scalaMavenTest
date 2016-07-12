package com.visenergy.scalaMavenTest.SparkSql

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext



object SparkSql2 {
  
    case class Person(name: String, age: Long)
    def main(args:Array[String]){
    val conf = new SparkConf().setAppName("SQLOnSpark").setMaster("local")
    val sc = new SparkContext(conf)
    //case class Person(name: String, age: Long)
    //val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    import sqlContext.implicits._
        
    val people = sc.textFile("test/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
    people.toDF().registerTempTable("people")
    val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
   }
}