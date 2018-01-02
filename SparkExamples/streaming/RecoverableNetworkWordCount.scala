package streaming

import java.io.File
import java.nio.charset.Charset

import com.google.common.io.Files
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.util.LongAccumulator


object RecoverableNetworkWordCount {

  object WordBlacklist {
    @volatile private var instance: Broadcast[Seq[String]] = _

    def getInstance(sc: SparkContext): Broadcast[Seq[String]] = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            val blacklist = Seq("a", "b", "c")
            instance = sc.broadcast(blacklist)
          }
        }
      }
      instance
    }
  }

  object DroppedWordsCounter {
    @volatile private var instance: LongAccumulator = _

    def getInstance(sc: SparkContext): LongAccumulator = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            instance = sc.longAccumulator("WordsInBlacklistCounter")
          }
        }
      }
      instance
    }
  }

  def createStreamingContext(checkpointDirectory: String, ip: String, port: Int, outputPath: String): StreamingContext = {
    println("Creating new streaming context")
    val conf = new SparkConf().setMaster("local[2]").setAppName("RecoverableNetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(10))

    //checkpoint能记录业务逻辑，不能将业务逻辑与ssc的checkpoint分离，否则无法进行recover
    ssc.checkpoint(checkpointDirectory)

    val outputFile = new File(outputPath)
    if (outputFile.exists()) {
      outputFile.delete()
    }

    val lines = ssc.socketTextStream(ip, port)
    val wordCounts = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    wordCounts.foreachRDD { (rdd: RDD[(String, Int)], time: Time) =>
      val blacklist = WordBlacklist.getInstance(rdd.sparkContext)
      val droppedWordsCounter = DroppedWordsCounter.getInstance(rdd.sparkContext)

      //统计存在的key值数据，并将其过滤掉
      val counts = rdd.filter { case (key, count) =>
        if (blacklist.value.contains(key)) {
          droppedWordsCounter.add(count)
          false
        } else {
          true
        }
      }.collect().mkString("[", ",", "]")

      val output = "Counts at time " + time + " " + counts
      println(output)
      println("Dropped " + droppedWordsCounter.value + " word(s) totally")
      println("Appending to " + outputFile.getAbsolutePath)
      Files.append(output + "\n", outputFile, Charset.defaultCharset())
    }

    ssc
  }

  def main(args: Array[String]): Unit = {

    val ip = "10.108.240.11"
    val port = 9998
    val checkDir = "hdfs://10.108.240.12:8020/test/wang/check"
    val output = "D:\\SparkApps2.2.1\\data\\file.txt"
    val ssc = StreamingContext.getOrCreate(checkDir, () => createStreamingContext(checkDir, ip, port, output))
    ssc.start()
    ssc.awaitTermination()
  }
}