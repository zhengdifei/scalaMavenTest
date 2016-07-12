package com.visenergy.scalaTest.firstDay
/**
 * Scala的元组结合件多个固定数量在一起，使它们可以被传来传去作为一个整体。
 * 不像一个数组或列表，元组可以容纳不同类型的对象，但它们也是不可改变的
 * 对于每个TupleN类型，其中1<= N <= 22
 */
object TupleTest {
  def main(args:Array[String]){
    val t = (1,2,3,4,5,"hello",Console)
    println(t)
    
    //要访问的元组 t 的元素
    println(t._1 + t._2 + t._3)
    
    //遍历
    t.productIterator.foreach { i => println("value: " + i) }
    
    //转换成字符串
    println(t.toString())
    
    //交换元素
    var t2 = new Tuple2("hello","world")
    println(t2.swap)
    //List
    var list = List[Int]()
    for(i <- 1 to 10){
      list ::= i 
    }
    var n = list.length - 1
    while(n>=0){
      println("a:" + n+ "v:" + list(n))
      n = n -1 
    }
  }
}