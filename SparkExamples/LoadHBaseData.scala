package demo

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

object LoadHBaseData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("TestHBase2")
    val sc = new SparkContext(conf)

    //设置hbase的zk参数
    val tableName = "test6"
    val hbaseConf = HBaseConfiguration.create()
    //hbaseConf.set("hbase.zookeeper.quorum", "10.108.240.103,10.108.240.105,10.108.240.107")
    hbaseConf.set("hbase.zookeeper.quorum", "10.108.240.13,10.108.240.15,10.108.240.17")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)


    // 如果表不存在则创建表
    val admin = new HBaseAdmin(hbaseConf)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
      admin.createTable(tableDesc)
    }

    //读取数据并转化成rdd
    val hbaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    println(hbaseRDD.count())

    hbaseRDD.foreach { case (_, result) =>
      //获取行键
      val key = Bytes.toString(result.getRow)
      //通过列族和列名获取列
      val name = Bytes.toString(result.getValue("cf".getBytes, "name".getBytes))
      val age = Bytes.toString(result.getValue("cf".getBytes, "age".getBytes))
      println("Row key:" + key + " Name:" + name + " Age:" + age)
    }

    sc.stop()
    admin.close()
  }
}
