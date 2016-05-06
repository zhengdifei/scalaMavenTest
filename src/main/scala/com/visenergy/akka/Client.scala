package com.visenergy.akka
/**
 *  1.    Client启动之后，亦作为一个Remote Actor;
 *	2.    接收到请求者的消息类型为AkkaMessage之后，将其转发至Server端；
 *	3.    接收到Server端返回的Response之后，将其响应给消息请求者；
 *	4.    请求者在向Client端的Actor发送一个AkkaMessage消息之后，等待响应之后，再继续发送下一个消息；
 */
import akka.actor.ActorSelection
import akka.actor.Actor
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import akka.actor.Props
import scala.concurrent.Await
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent._
import scala.concurrent.Future
import Server._
import akka.pattern.ask

class Client extends Actor{
  //远程Actor
  var remoteActor : ActorSelection = null
  //当前Actor
  var localActor : akka.actor.ActorRef = null
  
  override def preStart():Unit = {
    remoteActor = context.actorSelection("akka.tcp://zdf@127.0.0.1:2555/user/server")
    println("远程服务器地址：" + remoteActor)
  }
  
  override def receive : Receive ={
    case msg : AkkaMessage => {
      println("客户端发送消息： " + msg)
      this.localActor  = sender()
      remoteActor ! msg
    }
    case res : Response => {
      localActor ! res
    }
    case _ => println("客户端不支持的消息类型")
  }
  
}


object Client {

  def main(args: Array[String]): Unit = {
    val clientSystem = ActorSystem("ClientSystem",ConfigFactory.parseString("""
    		akka{
    			actor{
    				provider = "akka.remote.RemoteActorRefProvider"
    			}
    		}
    """))
    
    var client = clientSystem.actorOf(Props[Client])
    var msgs = Array[AkkaMessage](AkkaMessage("message1"),AkkaMessage("message2"),AkkaMessage("message3"),AkkaMessage("message4"))
    
    implicit val timeout = Timeout(3 seconds)
    //！意思是“launch后不管”，如发送和 返回，又称出来。
    //？发送 并返回一个代表一个可能的答复的未来。，又称问。
    msgs.foreach{x =>
    	val future = client ? x
    	val result = Await.result(future,timeout.duration).asInstanceOf[Response]
    	println("收到反馈：" + result)
    }
//    
//    msgs.foreach{x =>
//    	client ! x
//    }
    
    clientSystem.shutdown()
  }
  
}