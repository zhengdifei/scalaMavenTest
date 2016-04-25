package com.visenergy.scalaTest.firstDay
/**
 * Scala中的映射是键/值对的集合。
 * 任何值可以根据它的键进行检索。键是在映射唯一的，但值不一定是唯一的。
 * 映射也被称为哈希表。有两种类型的映射，不可变以及可变的。
 * 可变和不可变的对象之间的区别在于，当一个对象是不可变的，对象本身不能被改变。
 * 默认情况下，Scala中使用不可变的映射。如果想使用可变集，必须明确地导入scala.collection.mutable.Map类。
 * 如果想在同一个同时使用可变和不可变的映射，那么可以继续参考不可变的映射作为映射，但可以参考可变集合为mutable.Map。
 */

object MapTest {
  def main(args:Array[String]){
    val cols = Map("red" -> "#FF0000",
                       "azure" -> "#F0FFFF",
                       "peru" -> "#CD853F")
    val nums:Map[Int,Int] = Map()
    
    println(cols.keys)
    println(cols.values)
    println(cols.isEmpty)
    println(nums.isEmpty)
  }
}