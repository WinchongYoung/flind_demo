package test

object Test1 {

  // 成员变量初始化默认值
  // Int     -> 0
  // Double  -> 0.0
  // Boolean -> false
  // String  -> null
  // Object  -> null
  // val常量  -> 不能用 _
  var aaa: A = _

  def main(args: Array[String]): Unit = {
    // print(aaa)
    val a: Int = 10
    val b: Int = 9
  }

  class A() {

  }

}
