package com.dt.spark.demo

import scala.collection.mutable.ListBuffer

object ScalaListBuffer {
  def main(args: Array[String]): Unit = {
    // 创建可变集合List
    var fruits = new ListBuffer[String]()
    // 添加单个元素
    fruits += "Apple"
    fruits += "Orange"
    fruits += "Banana"
    //添加集合
    fruits += ("Kiwi", "Pineapple")
    //移除单个元素
    fruits -= "Apple"
    //移除集合
    fruits -= ("Orange", "Banana")
    //转换成List
    fruits.toList
    fruits.foreach(println)
    println(fruits(1))

    //list添加元素
    var list = List(1, 2)
    list = 3 +: list //List(3,1,2)
    list = list :+ 4 //List(3,1,2,4)
    list = list ::: List(5, 6, 7) // List(3,1,2,4,5,6,7)

    //list拼接
    val list1 = List(1, 2, 3)
    val list2 = List(4, 5, 6)
    val list3 = list1 ++ list2
    val list4 = list1 ::: list2
    val list5 = List.concat(list1, list2)

    //list排序
    list = list.sorted
    list = list.sortWith(_ > _) //倒序排序
    list = list.sortWith(_ < _) //顺序排序
    list.sortBy(x => x) //顺序排序
    list.sortBy(x => -x) //倒序排序

    //Array数组排序
    val array = Array("cherry", "apple", "banana")
    //对数组进行排序
    scala.util.Sorting.quickSort(array)
    array.sortBy(x => (x.length, x.head))
    array.sorted


  }
}
