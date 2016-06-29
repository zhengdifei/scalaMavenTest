package com.visenergy.streaming

object StreamingConfig extends Serializable {
  val CHECKPOINT_DIR:String = "/home/vis9/projects/checkpoint"
//  val CHECKPOINT_DIR:String = "hdfs://node1:9000/checkpoint"
  val STREAMING_INTERVAL:Int = 6 
  val REDIS_IP:String = "node13"
  val REDIS_PORT:Int = 6379 
  val KAFKA_BROKER_LIST = "node13:9092,node14:9092"
  val HBASE_ZK_IP:String = "node3"
  val HBASE_ZK_PORT:Int = 2181
  val HBASE_TABLE = "sensorData"
  val CITY_CLASSIFICATION_PROPERTY = "city"
  val BRAND_CLASSIFICATION_PROPERTY = "brand"
  val AMOUNT_PROPERTY = "price"
}