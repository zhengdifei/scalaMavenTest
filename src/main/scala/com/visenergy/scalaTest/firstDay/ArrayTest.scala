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
    //List学习，两个冒号(:)和三个冒号(:)的使用
    val one = List(1,2,3,4,5,6)
    val two = List(6,7,8,9,10)
    
    val three = one:::two
    println(three)
    
    val four = 1 :: three
    println(four)
  }
}