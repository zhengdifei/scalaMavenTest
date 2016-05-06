package com.visenergy.scalaTest.secondDay
/**
 * 通过异步消息通讯，可异步并发。要比Java多线程模型并发性能更好，彻底杜绝了共享锁
 */
case object Hop
case object Stop

case class Bunny(id:Int) extends scala.actors.Actor{
    //scala不能在方法内进行对象初始化，放到方法之外，类中，
    //!感叹号是发送消息，然后调用start()开始消息不断循环检查。
	this ! Hop
	start()
	//act()方法是一个自动的 match方式
	def act(){
	  //loop{ react{看上去怪，但是scala的关键,它实际是一个协调的多任务，每个任务做点事情然后放弃控制权，让下一个任务，这种方式好处没有共享内存，因此可以拓展伸缩，可以达数百万个任务。
	  loop{
	    react{
	      case Hop =>
	        println(this + " ")
	        this ! Hop
	        Thread.sleep(2000)
	      case Stop =>
	        println("Stopping " + this)
	        exit()
	    }
	  }
	}
}

object ActorTest {

  def main(args: Array[String]): Unit = {
    val bunnies = Range(0,10).map(new Bunny(_))
    println("press RETURN to quit")
    readLine
    bunnies.foreach(_ ! Stop)
  }

}