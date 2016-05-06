package com.visenergy.akka
/**
 * Spark中的RPC是使用Akka实现的，Akka的设计目标就是为分布式，
 * Actor之间的交互都是通过消息，并且所有动作都是异步的。
 * 在Spark应用程序中会有需要实现RPC的功能，比如：从一个一直运行的Spark Streaming应用程序中实时获取一些数据，
 * 或者更新一些变量等等，这时候，可以使用Akka的Remote Actor，
 * 将Spark Streaming应用程序作为一个Remote Actor（Server），
 * 当需要获取数据时候，可以向Server发送请求消息，Server收到请求并处理之后，将结果返回给客户端，
 * 需要注意的是，客户端向Server发送消息之后，
 * 需要等待Server将数据响应回来之后，才能处理并退出，这里的客户端，对于Server其实也是一个Remote Actor。
 * 
 * 本文学习和介绍使用Scala基于Akka的Remote Actor实现的简单RPC示例程序。
 */
import akka.actor.Actor
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.Props

case class AkkaMessage(message:Any)
case class Response(response:Any)

class Server extends Actor {
  override def receive : Receive = {
    case msg : AkkaMessage => {
      println("服务器端收到消息：" + msg.message )
      sender ! Response("response_" + msg.message )
    }
    
    case _ => println("服务器端不支持的消息类型……")
  }
}

object Server {
  //创建远程Actor：ServerSystem
  def main(args: Array[String]): Unit = {
    val serverSystem = ActorSystem("zdf",ConfigFactory.parseString("""
    		akka {
    			actor {
    				provider = "akka.remote.RemoteActorRefProvider"
    			}
    			remote {
    				enabled-transports = ["akka.remote.netty.tcp"]
    				netty.tcp {
    					hostname = "127.0.0.1"
    					port = 2555
    				}
    			}
    		}
    """))
    
    serverSystem.actorOf(Props[Server], "server")
  }

}