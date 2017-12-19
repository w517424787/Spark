package com.csot.spark.demo

import org.apache.spark.sql.SparkSession

/**
  * 设置blockSize的大小
  */

object BroadcastTest {
  def main(args: Array[String]): Unit = {

    val blockSize = if (args.length > 2) args(2) else "4096"
    val spark = SparkSession.builder().appName("BroadcastTest").master("local[2]")
      .config("spark.broadcast.blockSize", blockSize).getOrCreate()
    val sc = spark.sparkContext

    val slices = if (args.length > 0) args(0).toInt else 2
    val num = if (args.length > 1) args(1).toInt else 1000000
    val arr = (0 until num).toArray

    for (i <- 0 until 3) {
      println("Iteration " + i)
      val startTime = System.nanoTime()
      val broadArr = sc.broadcast(arr) //广播数组arr
      val observedSizes = sc.parallelize(1 to 10, slices).map(_ => broadArr.value.length) //将RDD中的每个值替换为数组的长度
      observedSizes.collect().foreach(println)
      println("Iteration %d took %.0f milliseconds".format(i, (System.nanoTime - startTime) / 1E6))
    }
	
    sc.stop()
    spark.stop()
  }
}
