package com.visenergy.rdd.release

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json.JSONObject
import scala.collection.immutable.List

class RddOpUtil extends Serializable {
  
  /**
   * query the total count of messages
   */
  def getMsgCount(input:RDD[String]) : Long = { 
    input.count()
  }
  
  def getAvgByKey(input:RDD[String], key:String) : Double = {
    getSumByKey(input, key).toDouble/getMsgCount(input)
  }
  
  /**
   * query the total bytes of messages
   */
  def getMsgBytesCount(sc:SparkContext, input:RDD[String]) : Long = {
    val sum = sc.accumulator(0L, "MsgBytesAccumulator")
    input.foreach(line => sum += line.getBytes().length)
    sum.value
  }
  
  /**
   * query the total count of messages by the given key
   */
  def getMsgCountByKey(input:RDD[String], key:String, value:String) : Long = {
    val filterRdd = input.filter(line => (new JSONObject(line).get(key) == value)) 
    filterRdd.count()
  }
  
  /**
   * query the total bytes of messages by the given key
   */
  def getMsgBytesCountByKey(sc:SparkContext,input:RDD[String], key:String, value:String) : Long = {
    val filterRdd = input.filter(line => (new JSONObject(line).get(key) == value)) 
    val sum = sc.accumulator(0L, "MsgBytesByKeyAccumulator")
    filterRdd.foreach(line => sum += line.getBytes().length)
    sum.value
  }
  
  /**
   * query sum of the given key
   */
  def getSumByKey(input:RDD[String], key:String) : Long = {
    input.map(line => (new JSONObject(line)).get(key).toString().toLong).reduce(_ + _)  
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
  
  /**
   * sort by the given key
   */
  def sortByKey(input:RDD[String], key:String) : RDD[String] = {
    val mapRdd = input.map(line => ((new JSONObject(line)).get(key).toString().toLong, line))
    val sortedRdd = mapRdd.sortByKey(true)
    sortedRdd.map(_._2)
  }
  
  
  /**
   * 新的分区合并取中位数方法
   */
  def getSectionNumNew(input:RDD[String], key:String, sectionValue:Double) : Long = {
    val mapRdd = input.map(line => ((new JSONObject(line)).get(key).toString().toLong))
    val msgCount = mapRdd.count()
    
    var msgPos = 1L
    if (sectionValue > 0) 
      msgPos = ((msgCount + 1) * sectionValue).toLong
    
    if (msgPos > msgCount) 
      msgPos = msgCount
      
    partitionRdNew(mapRdd, msgPos)
  }
  
  def partitionRdNew(input:RDD[Long], msgPos:Long) : Long = {
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
   * 查询标签的排行榜
   */
  def getMsgRank(input:RDD[String],key:String) : RDD[(String, Int)] = {
    val countRdd = input.map(line => ((new JSONObject(line)).get(key).toString(), 1)).reduceByKey(_ + _)
    countRdd.map(line => (line._2, line._1)).sortByKey(false).map(e => (e._2, e._1))
  }
  
  /**
   * 新的获取时间跨度区间的数据分布
   */
  def getTimeSpanDistributionNew(input:RDD[String], timeKey:String, targetKey:String,
      startTime:Long, endTime:Long, nodesNum:Long) : RDD[(Long, Long)] = {
    input.map(line => dataFilter(line, timeKey, targetKey, startTime, endTime, nodesNum)).reduceByKey(_ + _).sortByKey()
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
  
  /**
   * 合并分区获取中位数
   */
  def getSectionNum(input:RDD[String], key:String, sectionValue:Double) : Long = {
    val mapRdd = input.map(line => ((new JSONObject(line)).get(key).toString().toLong))
    val resultRdd = partitionRdd(mapRdd)
    val msgCount = resultRdd.count() - 1
    
    var msgPos = 1L
    if (sectionValue > 0) 
      msgPos = ((msgCount + 1) * sectionValue).toLong
    
    if (msgPos > msgCount) 
      msgPos = msgCount
      
    resultRdd.filter(line => line._1 == msgPos).collect().apply(0)._2
  }
  
  def partitionRdd(input:RDD[Long]) : RDD[(Long, Long)] = {
    input.coalesce(1).sortBy(x=>x, true).mapPartitions { 
      iter => {
         var pos = 1L
         var result = List((0L,0L))
		     while (iter.hasNext) {
		        result = (pos, iter.next) :: result
		        pos += 1
		     }
    	   result.iterator
      }
    }
  }
  
  /**
   * 在driver中获取中位数50%， 95%， 98%
   */
  def getSectionNum2(input:RDD[String], key:String, sectionValue:Double) : Long = {
    val mapRdd = input.map(line => ((new JSONObject(line)).get(key).toString().toLong))
    val sortedNumRdd = mapRdd.sortBy(x=>x, true)
    
    val msgCount = getMsgCount(input)
    
    val pos = ((msgCount + 1) * sectionValue).toInt
    
    sortedNumRdd.collect().apply(pos) 
  }
  
  /**
   * 用递归分区获取中位数
   */
  def getMedianNum2(input:RDD[String], key:String, numPartitions:Int) : Long = {
    val mapRdd = input.map(line => ((new JSONObject(line)).get(key).toString().toLong))
    val resultRdd = partitionRdd2(mapRdd,numPartitions)
    val medianNum = ((resultRdd.count() + 1)/2).toInt
    resultRdd.collect().apply(medianNum)
  }
  
  def partitionRdd2(input:RDD[Long], numPartitions:Int) : RDD[Long] = {
//    repartitionAndSortWithinPartitions
//    val result = input.repartition(numPartitions).sortBy(x=>x, true).mapPartitionsWithIndex { 
    val result = input.sortBy(x=>x, true, numPartitions).mapPartitionsWithIndex {  
      (partIdx,iter) => {
    	   var result = List[Long]()
    	   if(partIdx == (numPartitions - 1)/2) {
    		    while (iter.hasNext) {
    		      result ::= (iter.next)
    		    }
    	   }
    	   result.iterator
      }
    }
    
    if (result.count() <= numPartitions * 100) {
      result
    } else {
      partitionRdd2(result, numPartitions)
    }
  }
 }