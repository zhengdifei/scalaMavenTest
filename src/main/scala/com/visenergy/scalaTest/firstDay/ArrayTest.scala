package com.visenergy.scalaTest.firstDay
/**
 * 存储相同类型的元素的固定大小的连续集合
 */
object ArrayTest {
  def main(args:Array[String]){
    val myList = Array(1.9,2.1,3.5,2.6,8.2,1.3)
    //遍历一
    for(x <- myList){
      println(x)
    }
    //遍历二
    var total = 0.0;
    for(i <-0 to (myList.length - 1)){
      total += myList(i)
    }
    
    println(total)
  }
}