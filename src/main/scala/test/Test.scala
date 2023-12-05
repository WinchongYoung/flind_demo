package test

object Test {

  def main(args: Array[String]): Unit = {
    val ints: Array[Int] = Array[Int](0, 1, 2)

    ints match {
      case Array(1, 2, 3) => println("这个数组有1，2，3。")
      case Array(_, _) => println(s"这个数组包含两个元素。")
      case Array(0, _*) => println(s"这个数组以0开头。")
      case Array(1, 2, 3, _*) => println(s"这个数组以1，2，3开头。")
      case _ => println("都不匹配规则。")
    }

  }

}