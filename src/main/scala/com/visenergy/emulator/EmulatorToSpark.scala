package com.visenergy.emulator

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import scala.util.parsing.json.JSON
import redis.clients.jedis.JedisPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.json.JSONObject
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD
import org.json.JSONArray
import org.apache.spark.Accumulator
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import java.io.ByteArrayOutputStream
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.client.Result

/**
 * 在EumlatorToSpark4的基础上，用SparkSQL进行计算
 */
object EmulatorToSpark {
  //将json数组字符串变成json对象字符串
  def parseJsonArrStr2JsonObjStr(jsonArrayStr:String):Array[String] = {
    val transformOne = jsonArrayStr.replace("[", "").replace("]", "").replace("},{", "}&&{")
    transformOne.split("&&")
  }
  
  def parseJsonStr2JsonObj(jsonStr:String):JSONObject = {
    try{
    	new JSONObject(jsonStr)
    }catch{
      case ex: Exception => {
        println("JsonStr change to JsonObject error")
        //发送异常则，返回一个空JSON对象
        new JSONObject()
      }
    }
  }
  
  //获取hbase任务配置
  def getHbaseConfig(zkIp:String="localhost",zkPort:String="2181"):Configuration = {
    try{
	    val conf = HBaseConfiguration.create()
		conf.set("hbase.zookeeper.quorum",zkIp)
	    conf.set("hbase.zookeeper.property.clientPort", zkPort)
	    conf
    }catch{
      case ex: Exception => {
        println("create Hbase connect error")
        null
      }
    }
  }
  //数据存入hbase
  def insert2HbaseJobConf(config:Configuration,tableName:String):JobConf = {
    val jobConf = new JobConf(config,this.getClass())
		jobConf.setOutputFormat(classOf[TableOutputFormat])
		jobConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
		
		jobConf
  }
  //将rdd内对象，以json数组字符串形式存储rowkey为时间
  def hbaseAdaptor(acc_num:Accumulator[Int],acc_bytes:Accumulator[Long],currentTime:Long,jsonArrayStr:String) = {
    try{
		val jsonArray = new JSONArray(jsonArrayStr)
	    val p = new Put(Bytes.toBytes(currentTime.toString))
		for(i <- 0 to jsonArray.length -1){
		  val jsonObj = jsonArray.getJSONObject(i)
		  p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(jsonObj.getString("SID")+"_"+i), Bytes.toBytes(jsonObj.toString()))
		  //求价格的中位数，将价格单独作为一族
		  p.addColumn(Bytes.toBytes("price"), Bytes.toBytes(jsonObj.getString("SID")+"_"+i), Bytes.toBytes(jsonObj.getLong("price")))
		  
		  acc_bytes += jsonObj.getLong("price")
		}
		acc_num += jsonArray.length
		
	  	(new ImmutableBytesWritable,p)
    }catch{
    	case ex: Exception => {
    		println("create Hbase Object error")
    		null
      }
    }
  }
  
  //将scan转化成字符串，代码来源自org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
  def convertScanToString(scan:Scan) : String = {
    val proto = ProtobufUtil.toScan(scan);
    Base64.encodeBytes(proto.toByteArray());
  }
  
  //获取所有价格数据
  def allPriceFromHbase(sc:SparkContext,config:Configuration):RDD[(ImmutableBytesWritable,Result)] = {
    val scan = new Scan()
    scan.setStartRow(Bytes.toBytes("1464071076010"))
    scan.setStopRow(Bytes.toBytes("1464078947536"))
    scan.addFamily(Bytes.toBytes("price"))
    config.set(TableInputFormat.INPUT_TABLE, "sensorData")
    config.set(TableInputFormat.SCAN, convertScanToString(scan))
   
    
    sc.newAPIHadoopRDD(config, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])
  }
  
   /**
   * 迭代求中位数
   * rdd:数据集合
   * partNum : rdd分片数
   * midPartNum : 中间分片index
   * midCountNum : 中位数下标
   */
  def getMidNum(acc:Accumulator[Int],rdd:RDD[Double],partNum:Int,midPartNum:Int,midCountNum:Int):(RDD[Double],Int) = {
	  //中位数在第一个分区中,或者中位数落在最后一个分区中，数据分配不均
	  if(midPartNum == 0){
	    return (rdd,midCountNum)
	  }
	  //求中位数
	  val rdd1 = rdd.sortBy(x=>x, true, partNum)
	  //累加器初始化
	  acc.setValue(0)
	  //迭代缩小范围
	  val rdd2 = rdd1.mapPartitionsWithIndex{
	      (partIndex,iter) => {
	    	    //如何打印iter.size，则acc1，rdd9.count一直为0
			    //println(partIndex + ":" + iter.size)
			    var result = List[Double]()
			    if(partIndex == midPartNum){
			      while(iter.hasNext){
			    	  result ::=(iter.next)
			      }
			    }
			    if(partIndex < midPartNum){
			      acc += iter.size
			    }
			    result.iterator
	      }
	  }
	  //子结合数目
	  val rdd2Count = rdd2.count
	  //返回结果
	  var result = (rdd2,midCountNum-acc.value-1)
	  //中位数落在midPartNum小集合中
	  if(acc.value > midCountNum){
	      result = getMidNum(acc,rdd,partNum,midPartNum-1,midCountNum)
	  //中位数落在midPartNum大的机会中
	  }else if((acc.value + rdd2Count) < midCountNum){
	      result = getMidNum(acc,rdd,partNum,midPartNum+1,midCountNum)
	  //中位数正好落在midCountNum分区中
	  }else if(acc.value + rdd2Count >= midCountNum){
	     //如果发现子分区数量>20000,继续进行迭代，缩小范围
	     if(rdd2Count > 20000 ){
	    	 result = getMidNum(acc,rdd2,partNum,midPartNum,midCountNum-acc.value-1)
	     }
	  //其他异常情况
	  }else{
	    throw new Exception()
	  }
	  
	  result
  }
	  
  def main(args: Array[String]): Unit = {
    var sparkServer = "local[2]"
	if(args.length > 0 && args(0) != None){
	   sparkServer = args(0)
	}
    //sparkStreaming 初始化
    val sparkConf = new SparkConf().setAppName("EmulatorToSpark").setMaster(sparkServer)
    sparkConf.set("spark.sql.shuffle.partitions", "7")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(6))
    val sqlc = new SQLContext(sc)
    
    ssc.checkpoint("test/point")
    //初始化kafka direct kafka环境
    val topics = Set("sensorData")
    val kafkaParams = Map[String,String]("metadata.broker.list" -> "node13:9092,node14:9092")
    val sensorDataStreaming = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topics)
    
    //从hbase里面取price的全量数据
	val priceRddResult = allPriceFromHbase(sc,getHbaseConfig("node3"))
	var priceRdd = priceRddResult.flatMap(x => {
	  val fmap = x._2.getFamilyMap("price".getBytes())
	  var list = List[Double]()
	  val priceIter = fmap.values().iterator()
	  while(priceIter.hasNext()){
	    list::=(Bytes.toDouble(priceIter.next()))
	  }
	  list
	})
	//中位数累加器
    var acc_mid = sc.accumulator(0,"price_mid")
		    
    val sensorDs = sensorDataStreaming.map(x => x._2)
    //累加器内部原理：能够在rdd内部共享遍历，一个每个partition内部，都是从0开始分发，rdd之间是串行执行的，在rdd外部，是将时间轴上的累加值再进行累加
    val acc_sum = sc.accumulator(0,"sales_count")
    val acc_bytes = sc.accumulator(0L,"sales_amount")
    
    sensorDs.print()
    //foreacheRDD不是遍历rdd操作，后面的方法默认传入rdd参数，可以对RDD进行操作
    sensorDs.foreachRDD{ rdd => 
      if(!rdd.isEmpty){
	    try{
		    //数据存入hbase
	        val currentTime = System.currentTimeMillis()
		    rdd.map(hbaseAdaptor(acc_sum,acc_bytes,currentTime,_))saveAsHadoopDataset(insert2HbaseJobConf(getHbaseConfig("node3"),"sensorData"))
		    //用累加器统计消息模拟器发出数据总条数
			//初始化redis
		    RedisDao.makePool("node13")
			val redisClient = RedisDao.getPool.getResource()
		    //统计拦截器数量，字节数
			//redisClient.incrBy("sales_count", acc_sum.value)
			//redisClient.incrBy("sales_amount", acc_bytes.value)
	    	
			val rddJsonObjStr = rdd.flatMap(parseJsonArrStr2JsonObjStr)
			//统计其他信息
			val df1 = sqlc.jsonRDD(rddJsonObjStr)
			df1.registerTempTable("emulator")
			val df2 = sqlc.sql("select SID,count(SID) outlet_count,avg(price) outlet_avg_price,sum(price) outlet_amount from emulator group by SID")
			//因为数据量不大，所以将所有数据都汇总，不再每一个partition中创建redis
			df2.collect.foreach(row => {
			    //计算每一个模拟器数量，总价格，平均价格
				//redisClient.hincrBy("outlet_count",row.getAs[String]("SID") ,row.getAs[Long]("outlet_count"))
				//redisClient.hset("outlet_amount", row.getAs[String]("SID") , row.getAs[Long]("outlet_amount").toString)
				//redisClient.hset("outlet_avg_price", row.getAs[String]("SID") , row.getAs[Long]("outlet_avg_price").toString)
			})
			
			
//			//求price的中位数
//			val sPriceRdd = rddJsonObjStr.map(parseJsonStr2JsonObj).map(x=>x.getDouble("price"))
//			priceRdd = priceRdd.union(sPriceRdd)
//			val countNum = priceRdd.count
//			val partNum = 11
//		    val midPartNum = partNum/2 - 1 + partNum%2
//		    val midCountNum = countNum/2 + countNum%2
//		    val priceMidPartRdd = getMidNum(acc_mid,priceRdd,partNum,midPartNum,midCountNum.toInt)
//		    val priceMid = priceMidPartRdd._1.sortBy(x=>x).collect().apply((priceMidPartRdd._2).toInt)
//		    redisClient.set("mid_price", priceMid.toString)
		    //关闭redis
		    RedisDao.getPool.returnResource(redisClient)
	    }catch{
		    case ex: Exception => {
	    		println("foreach rdd error")
	    		null
		    }
	    }
      }
    }
    
    //利用window，计算一段时间销售排行
    val sensorWin1 = sensorDs.flatMap(parseJsonArrStr2JsonObjStr).map(parseJsonStr2JsonObj).map(rdd => (rdd.getString("SID"),rdd.getLong("price")))
    val sensorWin2 = sensorWin1.reduceByKeyAndWindow(_+_, _-_, Seconds(30), Seconds(12))
    
    sensorWin2.foreachRDD(rdd => {
       if(!rdd.isEmpty){
    	   try{
    	   	rdd.foreachPartition(iter => {
		        RedisDao.makePool("node13")
		        val redisClient = RedisDao.getPool.getResource()
		        while(iter.hasNext){
		          val oneSensor = iter.next
		          redisClient.hset("outlet_amount_30", oneSensor._1, oneSensor._2.toString)
		        }
		    	RedisDao.getPool.returnResource(redisClient)
		      })
	      }catch{
			    case ex: Exception => {
		    		println("foreach rdd error")
		    		null
			    }
		    }
       }
    })
    
    ssc.start()
    ssc.awaitTermination()
  }

}
