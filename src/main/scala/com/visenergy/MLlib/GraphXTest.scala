package com.visenergy.MLlib

import org.apache.spark.graphx.Edge
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexRDD

object GraphXTest {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("MLlibTest").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val vertexArray = Array((1L,("Alice",28)),(2L,("Bob",27)),(3L,("Charlie",65)),
        (4L,("David",42)),(5L,("Ed",55)),(6L,("Fran",50)))
        
    val edgeArray = Array(Edge(2L,1L,7),Edge(2L,4L,2),Edge(3L,2L,4),Edge(3L,6L,3),
        Edge(4L,1L,1),Edge(5L,2L,2),Edge(5L,3L,8),Edge(5L,6L,3))
        
    val vertexRdd : RDD[(Long,(String,Int))] = sc.parallelize(vertexArray)
    val edgeRdd : RDD[Edge[Int]] = sc.parallelize(edgeArray)
    
    val graph : Graph[(String,Int),Int] = Graph(vertexRdd,edgeRdd)
    println(s"Vertex:${graph.numVertices }\tEdge:${graph.numEdges }")
    
    graph.vertices.filter{
      case (id,(name,age)) => age > 50
    }.collect.foreach{
      case (id,(name,age)) => println(s"$name is $age")
    }
    
    for(triplet <- graph.triplets.collect){
      println(s"${triplet.srcAttr ._1 } => ${triplet.dstAttr._1 }")
    }
    
    //计算每个节点的出度和入度
    case class User(name:String,age:Int,inDeg:Int,outDeg:Int)
    val initialUserGraph : Graph[User,Int] = graph.mapVertices{
      case (id,(name,age)) => User(name,age,0,0)
    }
    val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees ){
      case (id,u,inDegOpt) => User(u.name ,u.age ,inDegOpt.getOrElse(0),u.outDeg )
    }.outerJoinVertices(initialUserGraph.outDegrees ){
      case (id,u,outDegOpt) => User(u.name,u.age,u.inDeg,outDegOpt.getOrElse(0))
    }
    userGraph.vertices.collect.foreach(v => 
    	println(s"${v._2 .name } inDeg:${v._2.inDeg } outDeg:${v._2 .outDeg }")
    )
    //获取每个用户年龄最小的关注者
    val oldestFollower : VertexRDD[(String,Int)] = userGraph.mapReduceTriplets[(String,Int)](
    	//Map过程遍历每条边给边所连接的目标节点发去源节点的信息
        edge => Iterator((edge.dstId ,(edge.srcAttr .name ,edge.srcAttr .age ))),
    	// reduce过程通过对比每个目标节点上的信息中源节点的年龄来获得最小的源节点
        (a,b) => if(a._2 > b._2) a else b
    )
    
    userGraph.vertices.leftJoin(oldestFollower){
      (id,user,optOldestFollower) => 
        optOldestFollower match {
          case None => s"${user.name } does not have any followers"
          case Some((name,age)) => s"${name} is the youngest follower of ${user.name }"
        }
    }.collect.foreach{ case (id,str) => println(str)}
    
    //获取满足某一条件的子图（年龄>30的子图）
    val olderGraph = userGraph.subgraph(vpred = (id,user) => user.age >= 30)
    
    for(triplet <- olderGraph.triplets.collect){
      println(s"${triplet.srcAttr .name } => ${triplet.dstAttr .name }")
    }
  }

}