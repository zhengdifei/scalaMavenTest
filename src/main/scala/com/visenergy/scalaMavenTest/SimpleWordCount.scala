package com.visenergy.scalaMavenTest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author ${user.name}
 */
object SimpleWordCount{

  def main(args : Array[String]) {
    val appName = "SimpleWordCount"
    val conf = new SparkConf().setAppName(appName).setMaster("local")
    val sc = new SparkContext(conf)
    
    // inputFile和stopWordFile表示了输入文件的路径，请根据实际情况修改
    val inputFiles = "test/SimpleWordCount.txt"
    val inputRDD = sc.textFile(inputFiles)
       
    def strSplit(s: String): Array[String] = {
        s.split("\\s+")
    }
    
    val inputRDDv1 = inputRDD.flatMap(strSplit)
    
    val inputRDDv2 = inputRDDv1.map(x => (x,1))
    
    val inputRDDv3 = inputRDDv2.reduceByKey(_ + _)
    inputRDDv3.saveAsTextFile("test/result1")
    
    val inputRDDv4 = inputRDDv3.map(x => x.swap)
    val inputRDDv5 = inputRDDv4.sortByKey(false)
    val inputRDDv6 = inputRDDv5.map(x => x.swap).keys
    val top5 = inputRDDv6.take(5)
    
    val outputFile = "test/result2"
    val result = sc.parallelize(top5)
    result.saveAsTextFile(outputFile)
  }

}
