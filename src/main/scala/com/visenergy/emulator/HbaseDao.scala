package com.visenergy.emulator

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.json.JSONObject
import org.json.JSONArray
/**
 * 创建Hbase操作集合
 * 异常信息：当我先运行kafka服务，在运行hbase服务，将出现错误：
 * Exception in thread "main" org.apache.hadoop.hbase.client.RetriesExhaustedException: Failed after attempts=36, exceptions:
 * 即使关闭kafka服务，错误不会消失
 */
object HbaseDao {
	/**
	 * Hbase创建初始化连接
	 * @param zkIp:zookeeper服务器Ip
	 * @param zkPort:zookeeper服务器端口号
	 * @result Connection:hbase连接
	 */
	def init(zkIp:String="localhost",zkPort:String="2181") : Connection = {
	    val conf = HBaseConfiguration.create()
	    conf.set("hbase.zookeeper.quorum","127.0.0.1")
	    conf.set("hbase.zookeeper.property.clientPort", "2181")
	    
	    val conn = ConnectionFactory.createConnection(conf)
	    conn
	}
	
	def createTable(tableName:String,conn:Connection,familys:JSONArray,isNew:Boolean=false):Unit = {
		val admin = conn.getAdmin()

	    val userTable = TableName.valueOf(tableName)
	    
	    //判断table是否已经存在，如果存成，是删除从新创建或者不做处理
	    if(admin.tableExists(userTable) && isNew){
	      admin.disableTable(userTable)
	      admin.deleteTable(userTable)
	      println(tableName + " exist,delete this table and create a new "+ tableName)
	    }else if(admin.tableExists(userTable)){
	      println(tableName + " exist,you can use the "+ tableName)
	    }
	    
	    //创建user表
	    val tableDesc = new HTableDescriptor(userTable)
	    for(i <- 0 until familys.length()){
	    	val oneFamily = new HColumnDescriptor(familys.getJSONObject(i).getString("familyName").getBytes())
	    	tableDesc.addFamily(oneFamily)
	    }
	    admin.createTable(tableDesc)
	    println("create success")
  }
  
  def main(args: Array[String]): Unit = {
      val conn = init()
      val familys = new JSONArray()
      
      //第一种方案
//      val sidJson = new JSONObject()
//      sidJson.put("familyName", "SID")
//      val snameJson = new JSONObject()
//      snameJson.put("familyName", "SNAME")
//      val uaJson = new JSONObject()
//      uaJson.put("familyName", "UA")
//      val iaJson = new JSONObject()
//      iaJson.put("familyName", "IA")
//      
//      familys.put(sidJson)
//      familys.put(snameJson)
//      familys.put(uaJson)
//      familys.put(iaJson)

      val cfJson = new JSONObject()
      cfJson.put("familyName", "cf")
      
      familys.put(cfJson)
      //第二种方案
      createTable("sensorData",conn,familys)
  }
}