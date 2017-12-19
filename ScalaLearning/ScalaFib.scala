package com.dt.spark.demo

object ScalaFib {

  /**
    * 求第n个斐波那契数列值
    *
    * @param n
    * @return
    */
  def fib(n: Int): Double = {
    n match {
      case 1 => 0
      case 2 => 1
      case _ => fib(n - 1) + fib(n - 2)
    }
  }

  def main(args: Array[String]): Unit = {
    println(fib(1))
    println(fib(2))
    println(fib(3))
    println(fib(4))
    println(fib(5))
    println(fib(6))
    println(fib(50))
  }
}
