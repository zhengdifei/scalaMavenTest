package com.visenergy.tdj.hbase

object HBaseConfig extends Serializable {
  val HBASE_ZK_IP:String = "localhost"
  val HBASE_ZK_PORT:String = "2181"
  val HBASE_TABLE:String = "sensorData"
  val COLUMN_FAMILY:String = "cf"
}