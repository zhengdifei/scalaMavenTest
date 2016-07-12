package com.visenergy.scalaTest.firstDay

object Helloworld{
  var name = "zhengdifei";
  
  val sex = true;
  
	def main(args:Array[String]):Unit = {
	  val array = Array("zdf","hello world");
	  array.foreach(println);
	  
	  var a,b = 0
	  a = a + 1 
	  println(a)
	  
	  val rate = "60%"
	  println(rate.substring(0,rate.length()-1))
	  
	  val args1 = "2007年01月"
	  val year = args1.substring(0, 4)
	  val month = args1.substring(5, 7)
	  println(year)
	  println(month)
	}
}