package com.dt.spark.demo

import scala.collection.mutable
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

    //创建多维数组
    val mulArray = Array.ofDim[String](2, 3)
    mulArray(0)(0) = "a"
    mulArray(0)(1) = "b"
    mulArray(0)(2) = "c"
    mulArray(1)(0) = "d"
    mulArray(1)(1) = "e"
    mulArray(1)(2) = "f"

    //读取单个值
    println(mulArray(1)(0))

    println("====================")
    //循环读取值
    for (i <- mulArray.indices) {
      for (j <- mulArray(i).indices) {
        println(mulArray(i)(j))
      }
    }

    println("====================")
    //可排序map,按key排序
    val sortMap = scala.collection.SortedMap("Kim" -> 90, "Al" -> 85, "Melissa" -> 95)
    for ((k, v) <- sortMap) {
      println(s"$k" + ":" + s"$v")
    }

    println("====================")
    //记录插入顺序的LinkedHashMap
    val linkedHaspMap = scala.collection.mutable.LinkedHashMap("Kim" -> 90)
    linkedHaspMap += ("Al" -> 85)
    linkedHaspMap += ("Melissa" -> 95)

    for ((k, v) <- linkedHaspMap) {
      println(s"$k" + ":" + s"$v")
    }

    println("====================")
    //可变Map
    val states = scala.collection.mutable.Map[String, Int]()
    states("Kim") = 90
    states += ("Al" -> 85)
    states ++= List(("Melissa", 95), ("Json", 70)) //添加元素集合

    for ((k, v) <- states) {
      println(s"$k" + ":" + s"$v")
    }

    println("====================")
    //删除元素
    states -= "Melissa" //("Al", "Json")
    states --= List("Al", "Json")

    for ((k, v) <- states) {
      println(s"$k" + ":" + s"$v")
    }

    println("====================")
    //对不可变集合进行增删操作，将集合定义为var
    var map = Map("Al" -> 95)
    map += ("Kim" -> 90)
    map += ("Melissa" -> 95, "Json" -> 70)
    //移除一个元素
    map -= "Kim"
    for ((k, v) <- map) {
      println(s"$k" + ":" + s"$v")
    }

    println("====================")
    //遍历集合并修改value值 mapValues
    map.mapValues(x => x * 2).foreach(x => println("key:" + x._1 + ",value:" + x._2))
    map.mapValues(x => x * 2).foreach(x => println(s"key:${x._1}" + s",value:${x._2}"))
    map.mapValues(x => x * 2).foreach { case (k, v) => println(s"key:$k" + s",value:$v") }

    println("====================")
    //获取集合中key或value
    val keys = map.keySet
    println(keys.head)

    println("====================")
    //key和value值进行反转
    val newMap = for ((k, v) <- map) yield (v, k)
    for ((k, v) <- newMap) {
      println(s"$k" + ":" + s"$v")
    }

    println("====================")
    //根据key或者value对映射进行排序
    val grades = Map("Kim" -> 90, "Al" -> 85, "Melissa" -> 95, "Emily" -> 91, "Hannah" -> 92)
    //排序时不会改变原映射，会产生新的映射
    scala.collection.immutable.ListMap(grades.toSeq.sortBy(_._1): _*) //升序
    //也可以通过sortWith来升序或降序排序
    scala.collection.immutable.ListMap(grades.toSeq.sortWith(_._1 > _._1): _*) //降序
    scala.collection.immutable.ListMap(grades.toSeq.sortWith(_._1 < _._1): _*) //升序

    //读取映射中key最大或者value最大的
    println(grades.max)
    println(grades.keysIterator.reduceLeft((x, y) => if (x > y) x else y))
    println(grades.keysIterator.max)
    println(grades.valuesIterator.max)
    println(grades.keysIterator.reduceLeft((x, y) => if (x.length > y.length) x else y))

    println("==========Queue==========")
    //队列
    var queue = scala.collection.mutable.Queue[String]()
    queue += "Kim"
    queue += ("Al", "Melissa")
    queue ++= List("Emily", "Hannah")

    //出队列
    queue.dequeue()
    queue.dequeueFirst(_.startsWith("M"))
    queue.dequeueAll(_.length > 5)
    queue.foreach(println)

    //Range
    (1 to 10).toList
    (1 to 10).toArray
  }
}