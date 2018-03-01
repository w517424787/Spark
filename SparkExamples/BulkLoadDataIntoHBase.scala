package demo

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

object BulkLoadDataIntoHBase {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("BulkLoadDataIntoHBase")
    val sc = new SparkContext(conf)
    val hbaseConf = HBaseConfiguration.create()
    val tableName = "test5"
    val table = new HTable(hbaseConf, tableName)
    //这个TableOutputFormat需要用org.apache.hadoop.hbase.mapred里面的
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    lazy val job = Job.getInstance(hbaseConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[KeyValue])
    HFileOutputFormat.configureIncrementalLoad(job, table)

    val rdd = sc.textFile("D:\\SparkApps2.2.1\\data\\data.txt")
      .map(_.split("@"))
      .map { x => (DigestUtils.md5Hex(x(0) + x(1)).substring(0, 3) + x(0) + x(1), x(2)) }
      .sortBy(_._1)
      .map(x => {
        val kv: KeyValue = new KeyValue(Bytes.toBytes(x._1),
          Bytes.toBytes("cf"), Bytes.toBytes("value"), Bytes.toBytes(x._2))
        (new ImmutableBytesWritable(kv.getKey), kv)
      })
    rdd.saveAsNewAPIHadoopFile("/tmp/test5",
      classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat], job.getConfiguration())
    val bulkLoader = new LoadIncrementalHFiles(hbaseConf)
    bulkLoader.doBulkLoad(new Path("/tmp/test5"), table)
  }
}
