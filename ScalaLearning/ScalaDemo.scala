package com.sdf.flink.scala99


object ScalaDemo {

  //List(List(1, 1), 2, List(3, List(5, 8)))
  def flatten(xs: List[Any]): List[Any] = xs match {
    case Nil => Nil
    case (head: List[_]) :: tail => flatten(head) ++ flatten(tail)
    case head :: tail => head :: flatten(tail)
  }

  def flatten2(xs: List[Any]): List[Any] = xs flatMap {
    case xs: List[_] => flatten2(xs)
    case e => List(e)
  }

  //If a list contains repeated elements they should be replaced with a single copy of the element.
  // The order of the elements should not be changed.
  // List('a', 'a', 'a', 'a', 'b', 'c', 'c', 'a', 'a', 'd', 'e', 'e', 'e', 'e')
  // List('a', 'b', 'c', 'a', 'd', 'e')
  def compress(list: List[Any]): List[Any] = {
    var flag: Any = null
    var result: List[Any] = List()
    list.foreach(x => {
      if (!x.equals(flag)) {
        result = result.:+(x)
        flag = x
      }
    })
    result
  }

  def pack[A](list: List[A]): List[List[A]] = {
    if (list.isEmpty) {
      List(List())
    } else {
      val (head, next) = list.span {
        _ == list.head
      }
      if (next == Nil) {
        List(head)
      } else {
        head :: pack(next)
      }
    }
  }

  def encode[A](list: List[A]): List[(Int, A)] = {
    pack(list) map { e => (e.length, e.head) }
  }

  def encodeModified[A](list: List[A]): List[Any] = {
    pack(list).map { e =>
      e.length match {
        case 1 => e.head
        case _ => (e.length, e.head)
      }
    }
  }

  def encodeModified2[A](list: List[A]): List[Any] = {
    encode(list).map { e => if (e._1 == 1) e._2 else e }
  }


  def main(args: Array[String]): Unit = {

    //P07 Flatten a nested list structure.
    //平铺List
    //val list = List(List(1, 1), 2, List(3, List(5, 8)))
    //println(flatten(list)) //List(1, 1, 2, 3, 5, 8)
    //println(flatten2(list)) //List(1, 1, 2, 3, 5, 8)

    //P08 (**) Eliminate consecutive duplicates of list elements.
    //println(compress(List('a', 'a', 'a', 'a', 'b', 'c', 'c', 'a', 'a', 'd', 'e', 'e', 'e', 'e')))
    //List(a, b, c, a, d, e)

    //P09 (**) Pack consecutive duplicates of list elements into sublists.
    //List('a, 'a, 'a, 'a, 'b, 'c, 'c, 'a, 'a, 'd, 'e, 'e, 'e, 'e)
    //List(List('a, 'a, 'a, 'a), List('b), List('c, 'c), List('a, 'a), List('d), List('e, 'e, 'e, 'e))
    //println(pack(List('a, 'a, 'a, 'a, 'b, 'c, 'c, 'a, 'a, 'd, 'e, 'e, 'e, 'e)))

    //P10 (*) Run-length encoding of a list.
    //List('a, 'a, 'a, 'a, 'b, 'c, 'c, 'a, 'a, 'd, 'e, 'e, 'e, 'e)
    //List((4,'a), (1,'b), (2,'c), (2,'a), (1,'d), (4,'e))
    //println(encode(List('a, 'a, 'a, 'a, 'b, 'c, 'c, 'a, 'a, 'd, 'e, 'e, 'e, 'e)))

    //P11 (*) Modified run-length encoding.
    //List('a, 'a, 'a, 'a, 'b, 'c, 'c, 'a, 'a, 'd, 'e, 'e, 'e, 'e)
    //List[Any] = List((4,'a), 'b, (2,'c), (2,'a), 'd, (4,'e))
    //println(encodeModified(List('a, 'a, 'a, 'a, 'b, 'c, 'c, 'a, 'a, 'd, 'e, 'e, 'e, 'e)))
    //println(encodeModified2(List('a, 'a, 'a, 'a, 'b, 'c, 'c, 'a, 'a, 'd, 'e, 'e, 'e, 'e)))

    //P12 (**) Decode a run-length encoded list.

  }
}
