package com.visenergy.scalaTest.firstDay

object Test1 {
  def addOne(m: Int): Int = {
    m + 1
  }
  //如果函数不带参数，调用的时候可以不写括号，函数体中的语句如果只有一条表达式，可以省略函数的大括号
  def addOne2(): Int = 1 + 1

  def add(x:Int,y:Int):Int = {
    x+y
  }
  //有时会有这样的需求：允许别人一会在你的函数上应用一些参数，然后又应用另外的一些参数。
  def addKlh(x:Int)(y:Int):Int={
    x + y
  }
  //这是一个特殊的语法，可以向方法传入任意多个同类型的参数
  def capitalizeAll(strs:String*) = {
    strs.map { x => x.capitalize }
  }
  
  def addAll(a:Int*) = {
    a.map { b => b+1 }
  }
  def main(args: Array[String]) {
    var res: Int = addOne(5)

    println(res)

    println(addOne2)
    //你可以使用下划线_部分应用一个函数,Scala使用下划线表示不同上下文中的不同事物，你通常可以把它看作是一个没有命名的神奇通配符
    var a1 = add(1,_:Int)
    println(a1)
    
    println(a1(2))
    //有时会有这样的需求：允许别人一会在你的函数上应用一些参数，然后又应用另外的一些参数。
    var add1 = addKlh(1)(2)
    var add2 = addKlh(1)_
    var add3 = add2(3)
    
    println(add1)
    println(add2)
    println(add3)
    
    println(capitalizeAll("hello","world"))
    
    println(addAll(1,3,5,7,9))
    
    val cal = new Calculator("zdf")
    println(cal.color)
    
    val scal = new ScientificCalculator("syl")
    println(scal)
    println(scal.color)
    println(scal.log(100, 20))
    
    val escal = new EvenMoreScientificCalculator("")
    println(escal)
    println(escal.color)
    println(escal.log(100))
    
    //集合
    val x1 = List(1,2,3,4)
    val x2 = Set(1,3,5)
    val x3 = Map("one"->1,"two"->2)
    val x4 = (3,"three")//定义元祖
    val x5:Option[Int] = Some(5)//定义选项
    
    println(x1)
    println(x2)
    println(x3)
    println(x4)
    println(x5)
    
    val aa1:Int = 5
    var aa2:Int = 7
    //aa1 = 6
    aa2 = 8
  }
}
//构造函数不是特殊的方法，他们是除了类的方法定义之外的代码。让我们扩展计算器的例子，增加一个构造函数参数，并用它来初始化内部状态
class Calculator(brand:String){
  //上文的Calculator例子说明了Scala是如何面向表达式的。颜色的值就是绑定在一个if/else表达式上的。Scala是高度面向表达式的：大多数东西都是表达式而非指令。
   var color : String = if(brand == "syl"){
    "blue"
  }else if(brand == "zdf"){
    "black"
  }else{
    "white"
  }
  
  def add(a:Int,b:Int):Int = a + b
}
//继承
class ScientificCalculator(brand:String) extends Calculator(brand){
  def log(m:Double,base:Double) = math.log(m)/math.log(base)
}
//重载方法
class EvenMoreScientificCalculator(brand:String) extends ScientificCalculator(brand){
  def log(m:Int):Double = log(m,math.exp(1))
}