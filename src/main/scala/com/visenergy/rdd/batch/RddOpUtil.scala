package com.visenergy.rdd.batch

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json.JSONObject
import scala.collection.immutable.List

class RddOpUtil extends Serializable {
  
  /**
   * 获取消息总数
   */
  def getMsgCount(input:RDD[String]) : Long = { 
    input.count()
  }
  
  /**
   * 获取消息总字节数
   */
  def getMsgBytesCount(sc:SparkContext, input:RDD[String]) : Long = {
    val sum = sc.accumulator(0L, "MsgBytesAccumulator")
    input.foreach(line => sum += line.getBytes().length)
    sum.value
  }
  
  /**
   * 获取给定属性值的平均数
   */
  def getAvgByKey(input:RDD[String], key:String) : Double = {
    getSumByKey(input, key).toDouble/getMsgCount(input)
  }
  
  /**
   * 获取给定属性值的消息数
   */
  def getMsgCountByKey(input:RDD[String], key:String, value:String) : Long = {
    val filterRdd = input.filter(line => (new JSONObject(line).get(key) == value)) 
    filterRdd.count()
  }
  
  /**
   * 获取给定属性值的消息字节数
   */
  def getMsgBytesCountByKey(sc:SparkContext,input:RDD[String], key:String, value:String) : Long = {
    val filterRdd = input.filter(line => (new JSONObject(line).get(key) == value)) 
    val sum = sc.accumulator(0L, "MsgBytesByKeyAccumulator")
    filterRdd.foreach(line => sum += line.getBytes().length)
    sum.value
  }
  
  /**
   * 获取给定属性值的总和
   */
  def getSumByKey(input:RDD[String], key:String) : Long = {
    input.map(line => (new JSONObject(line)).get(key).toString().toLong).reduce(_ + _)  
  }
  
  /**
   * 根据给定属性值进行排序
   */
  def sortByKey(input:RDD[String], key:String) : RDD[String] = {
    val mapRdd = input.map(line => ((new JSONObject(line)).get(key).toString().toLong, line))
    val sortedRdd = mapRdd.sortByKey(true)
    sortedRdd.map(_._2)
  }
  
  /**
   * 获取给定属性值的排行榜
   */
  def getMsgRank(input:RDD[String],key:String) : RDD[(String, Int)] = {
    val countRdd = input.map(line => ((new JSONObject(line)).get(key).toString(), 1)).reduceByKey(_ + _)
    countRdd.map(line => (line._2, line._1)).sortByKey(false).map(e => (e._2, e._1))
  }
  
  /**
   * 获取给定属性值对应目标属性的排行榜
   */
  def getMsgRank4TargetKey(input:RDD[String],key:String,targetKey:String) : RDD[(String, Double)] = {
    val countRdd = input.map(line => {
      val jsonObj = (new JSONObject(line))
      (jsonObj.get(key).toString(), jsonObj.get(targetKey).toString().toDouble)
    }).reduceByKey(_ + _)
    countRdd.map(line => (line._2, line._1)).sortByKey(false).map(e => (e._2, e._1))
  }
  
  /**
   * 根据给定属性值求中位数（50%,90%）
   */
  def getSectionNum(input:RDD[String], key:String, sectionValue:Double) : Long = {
    val mapRdd = input.map(line => ((new JSONObject(line)).get(key).toString().toLong))
    val msgCount = mapRdd.count()
    
    var msgPos = 1L
    if (sectionValue > 0) 
      msgPos = ((msgCount + 1) * sectionValue).toLong
    
    if (msgPos > msgCount) 
      msgPos = msgCount
      
    partitionRdd(mapRdd, msgPos)
  }
  
  def partitionRdd(input:RDD[Long], msgPos:Long) : Long = {
    input.coalesce(1).sortBy(x=>x, true).mapPartitions { 
      iter => {
         var result = List[Long]()
         var pos = 1L
		     while (iter.hasNext) {
		       val number = iter.next()
		       if (pos == msgPos) {
		         result ::= number
		       }
		       pos += 1
		     }
    	   result.iterator
      }
    }.collect().apply(0)
  }
  
  /**
   * 条件过滤
   */
  def conditionFilter(input:RDD[String], keyValuePairList:List[Tuple2[String, String]]) : RDD[String] = {
    if (keyValuePairList.length > 0) {
        input.filter(line => {
        var result = true
        val jsonObj = new JSONObject(line)
        for(tuple <- keyValuePairList) {
          result = result && (jsonObj.get(tuple._1).toString() == tuple._2)
        }
        result
      })
    } else {
      input
    }
    
  }
  
  /**
   * 获取标签的and操作结果集 
   */
  def getAndOpMsg(input:RDD[String], keyValuePairList:List[Tuple2[String, String]]) : RDD[String] = {
    var resultRdd = input.filter(line => {
      new JSONObject(line).get(keyValuePairList.apply(0)._1).toString() == keyValuePairList.apply(0)._2
    })
    
    val pairList = keyValuePairList.drop(0)
    pairList.foreach(pair => {
        val msgRdd = input.filter(line => new JSONObject(line).get(pair._1).toString() == pair._2)
        resultRdd = resultRdd.intersection(msgRdd)
      }
    )
    
    resultRdd
  }
  
  /**
   * 获取标签的and操作结果总数
   */
  def getAndOpMsgCount(input:RDD[String], keyValuePairList:List[Tuple2[String, String]]) : Long = {
    input.filter(line => {
      var result = true
      for(tuple <- keyValuePairList) {
        val jsonObj = new JSONObject(line)
        result = result && (jsonObj.get(tuple._1).toString() == tuple._2)
      }
      result
    }).count
  }
  
  /**
   * 获取标签的or操作结果集 
   */
  def getOrOpMsg(input:RDD[String], keyValuePairList:List[Tuple2[String, String]]) : RDD[String] = {
    var resultRdd = input.filter(line => {
      new JSONObject(line).get(keyValuePairList.apply(0)._1).toString() == keyValuePairList.apply(0)._2
    })
    
    val pairList = keyValuePairList.drop(0)
    pairList.foreach(pair => {
        val msgRdd = input.filter(line => (new JSONObject(line).get(pair._1) == pair._2))
        resultRdd = resultRdd.subtract(msgRdd).union(msgRdd)
      }
    )
    resultRdd
  }
  
   /**
   * 获取时间跨度区间的数据分布
   */
  def getTimeSpanDistribution(input:RDD[String], timeKey:String, targetKey:String,
      startTime:Long, endTime:Long, nodesNum:Long) : List[Tuple2[Long, Long]] = {
    var result = List[Tuple2[Long, Long]]()
    for(i <- 1L to nodesNum) {
      val startSpanTime = endTime - i * ((endTime - startTime) / nodesNum)
      val endSpanTime = endTime - (i-1) * ((endTime - startTime) / nodesNum)
      result ::= (nodesNum-i+1, {
        val filterRdd = input.filter(line => {
          val jsonValue = (new JSONObject(line)).get(timeKey).toString().toLong
          (jsonValue >= startSpanTime) && (jsonValue < endSpanTime)
        })
        if (filterRdd.isEmpty()) {
          0L
        } else {
          filterRdd.map(line2 => (new JSONObject(line2)).get(targetKey).toString().toLong).reduce(_ + _)
        }
      })
    }
    result
  }
  
  /**
   * 新的获取时间跨度区间的数据分布
   */
  def getTimeSpanDistributionNew(input:RDD[String], timeKey:String, targetKey:String,
      startTime:Long, endTime:Long, nodesNum:Long) : Array[Tuple2[Long, Long]] = {
    input.map(line => dataFilter(line, timeKey, targetKey, startTime, endTime, nodesNum)).reduceByKey(_ + _).collect()
  }
  
  private def dataFilter(line:String, timeKey:String, targetKey:String,
      startTime:Long, endTime:Long, nodesNum:Long) : Tuple2[Long, Long] = {
    var pos = 0L
    val jsonObj = new JSONObject(line)
    var resultTuple:Tuple2[Long, Long] = (1L, 0L)
    for(i <- 1L to nodesNum) {
      val startSpanTime = startTime + (i-1) * ((endTime - startTime) / nodesNum)
      val endSpanTime = startTime + i * ((endTime - startTime) / nodesNum)
      val jsonValue = jsonObj.get(timeKey).toString().toLong
      if ((jsonValue >= startSpanTime) && (jsonValue < endSpanTime)) {
        pos = i
      }
    }
     
    if (pos > 0) {
       resultTuple = (pos, jsonObj.get(targetKey).toString().toLong)
    }
    
    resultTuple
  }
  
  def getCountDistributionByTimeSpan(input:RDD[String], timeKey:String, 
      startTime:Long, endTime:Long, nodesNum:Long) : Array[Tuple2[Long, Long]] = {
    input.map(line => msgCountFilter(line, timeKey, startTime, endTime, nodesNum)).reduceByKey(_ + _).collect()
  } 
  
  private def msgCountFilter(line:String, timeKey:String,
      startTime:Long, endTime:Long, nodesNum:Long) : Tuple2[Long, Long] = {
    var pos = 0L
    val jsonObj = new JSONObject(line) 
    var resultTuple:Tuple2[Long, Long] = (1L, 0L)
    for(i <- 1L to nodesNum) {
      val startSpanTime = startTime + (i-1) * ((endTime - startTime) / nodesNum)
      val endSpanTime = startTime + i * ((endTime - startTime) / nodesNum)
      val jsonValue = jsonObj.get(timeKey).toString().toLong
      if ((jsonValue >= startSpanTime) && (jsonValue < endSpanTime)) {
        pos = i
      }
    }
     
    if (pos > 0) {
       resultTuple = (pos, 1L)
    }
    
    resultTuple
  }
  
  def getMsgCountDistribution(input:RDD[String], key:String) : Array[(String,Long)] = {
    input.map(line => ((new JSONObject(line)).get(key).toString(), 1L)).reduceByKey(_ + _).collect()
  }
  
  def getMsgSumDistribution(input:RDD[String], idKey:String, targetKey:String) : Array[(String, Long)] = {
    input.map(line => {
      val jsonObj = new JSONObject(line)
      (jsonObj.get(idKey).toString(), jsonObj.get(targetKey).toString().toLong)
    }).reduceByKey(_ + _).collect()
  }
  
 }