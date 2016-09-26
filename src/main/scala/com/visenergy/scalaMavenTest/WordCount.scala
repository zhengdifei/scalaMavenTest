package com.visenergy.scalaMavenTest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author ${user.name}
 */
object WordCount{

  def main(args : Array[String]) {
    val appName = "WordCount"
    val conf = new SparkConf().setAppName(appName).setMaster("local")
    val sc = new SparkContext(conf)
    
    // inputFile和stopWordFile表示了输入文件的路径，请根据实际情况修改
    val inputFiles = "test/shakespear/*"
    val stopWordFile = "test/stopword.txt"
    val inputRDD = sc.textFile(inputFiles)
    val stopRDD = sc.textFile(stopWordFile)
    
    val targetList: Array[String] = Array[String]("""\t\().,?[]!;|""")
    
    def replaceAndSplit(s: String): Array[String] = {
        for(c <- targetList)
          s.replace(c, " ")
        s.split("\\s+")
    }
    
    val inputRDDv1 = inputRDD.flatMap(replaceAndSplit)
    val stopList = stopRDD.map(x => x.trim()).collect()
    
    val inputRDDv2 = inputRDDv1.filter(x => stopList contains x)
    val inputRDDv3 = inputRDDv2.map(x => (x,1))
    
    val inputRDDv4 = inputRDDv3.reduceByKey(_ + _)
    inputRDDv4.saveAsTextFile("test/result3")
    
    val inputRDDv5 = inputRDDv4.map(x => x.swap)
    val inputRDDv6 = inputRDDv5.sortByKey(false)
    val inputRDDv7 = inputRDDv6.map(x => x.swap).keys
    val top100 = inputRDDv7.take(100)
    
    val outputFile = "test/result4"
    val result = sc.parallelize(top100)
    result.saveAsTextFile(outputFile)
  }

}
