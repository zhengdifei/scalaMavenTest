package com.visenergy.hbase

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
/**
 * 1.创建hbase连接
 * 2.创建表
 * 3.创建列
 * 4.读取列数据
 * 5.扫描整个表
 * 6.删除某行数据
 */
object HbaseHelloworld {
  def hbaseInit(zkIp:String="localhost",zkPort:String="2181") : Connection = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",zkIp)
    conf.set("hbase.zookeeper.property.clientPort", zkPort)
    
    val conn = ConnectionFactory.createConnection(conf)
    conn
  }
  
  def createTable(tableName:String,conn:Connection,isNew:Boolean=false):Unit = {
     val admin = conn.getAdmin();
    
    val userTable = TableName.valueOf(tableName)
    
    //创建user表
    val tableDesc = new HTableDescriptor(userTable)
    tableDesc.addFamily(new HColumnDescriptor("basic".getBytes()))
    println("create table 'user'")
    
    if(admin.tableExists(userTable) && !isNew){
      admin.disableTable(userTable)
      admin.deleteTable(userTable)
      println("delete exist table")
    }
    
    admin.createTable(tableDesc)
    println("create success")
  }
  
  
  def main(args: Array[String]): Unit = {
    //初始化hbase连接
	val conn = hbaseInit()
	
	try{
		//获取user表
		val table = conn.getTable(TableName.valueOf("user"))
		
		try{
		  //插入key=id001的数据
		  val p = new Put("id002".getBytes())
		  p.addColumn("basic".getBytes(), "name".getBytes(), "zhengdifei2".getBytes())
		  table.put(p)
		  println("Add key:id001")
		  println("----------------")
		  //查询某条数据
		  val g = new Get("id001".getBytes())
		  val result = table.get(g)
		  val value = Bytes.toString(result.getValue("basic".getBytes(), "name".getBytes()))
		  println("GET id001:" + value)
		  println("----------------")
		  //扫描数据
		  val s = new Scan()
		  s.addColumn("basic".getBytes(), "name".getBytes())
		  val scanner = table.getScanner(s)
		  
		  try{
			  val scanIter = scanner.iterator()
			  while(scanIter.hasNext()){
			    val r = scanIter.next()
			    println("row:" +  r)
			    println("value:" + Bytes.toString(r.getValue("basic".getBytes(), "name".getBytes())))
			  }
			  println("----------------")
			}finally{
			  scanner.close()
			}
			
			//删除某条数据
			val d = new Delete("id002".getBytes())
			d.addColumn("basic".getBytes(), "name".getBytes())
			//table.delete(d)
			println("Delete key id002")
		}finally{
		  if(table != null) table.close()
		}
	}finally{
		conn.close()
	}   
  }

}