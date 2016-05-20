package com.visenergy.rdd.release

object BaseConfig extends Serializable {
  /** hbase config **/
  val HBASE_ZK_IP:String = "localhost"
  val HBASE_ZK_PORT:Int = 2181
  val HBASE_TABLE:String = "sensorData"
  val COLUMN_FAMILY:String = "cf"
  
  /** redis config **/
  val REDIS_IP = "localhost"
  val REDIS_PORT = 6379 
  
  /** scheduler config **/ 
  // unit is second
  val SCHEDULER_INTERVAL = 6L                           
  val SCHEDULER_THREAD_POOL_SIZE = 200
  
  /** business property **/
  val SALES_AMOUNT_PROPERTY = "price" 
  val ID_PROPERTY = "sid"
  val DATA_DISTRIBUTION_KEY = "saleTime"
  val DATA_DISTRIBUTION_NODES = 12L
  val RANK_BY_KEY_FIRST_NUM = 10
  /** unit is second **/
  val REDIS_KEY_TIMEOUT = 60
  
}