package streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object StatefulNetworkWordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val hostName = if (args.length > 1) args(0) else "10.108.240.11"
    val port = if (args.length > 1) args(1).toInt else 9998

    val conf = new SparkConf().setMaster("local[2]").setAppName("StatefulNetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint("hdfs://10.108.240.12:8020/test/check/")

    //设定key的初始值
    val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))
    val lines = ssc.socketTextStream(hostName, port, StorageLevel.MEMORY_ONLY)
    val words = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    //定义更新累计值偏函数
    //key: 输入的值
    //value: 当前DStream中key值的个数
    //stateSum: 记录各key的累计值
    val mappingFunc = (key: String, value: Option[Int], stateSum: State[Int]) => {
      val sum = value.getOrElse(0) + stateSum.getOption().getOrElse(0)
      //更新key的累计值
      stateSum.update(sum)
      //输出当前key的最新结果
      (key, sum)
    }

    //更新每个key的累计值
    val stateDstream = words.mapWithState {
      StateSpec.function(mappingFunc).initialState(initialRDD)
    }

    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
