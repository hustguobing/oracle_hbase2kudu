package com.test.kudu

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import java.util.Base64
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.spark.sql.RowFactory
import java.util.ArrayList
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataTypes
import java.lang.Long

object HbaseTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse/spark-warehouse")
      .appName("HbaseTest")
      .master("local[*]")
      .getOrCreate()
    val sc = sparkSession.sparkContext

    val tableName = "db_user_tags_t_user_coupon_info"
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "172.16.9.102,172.16.9.102,172.16.9.102")
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)

    val scan = new Scan()
    scan.addFamily(Bytes.toBytes("f1"))
    val proto = ProtobufUtil.toScan(scan)
    val scanToString = new String(Base64.getEncoder.encode(proto.toByteArray()))
    hbaseConf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.SCAN, scanToString)

    //读取数据并转化成rdd TableInputFormat是org.apache.hadoop.hbase.mapreduce包下的
    val hbaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    //    val dataRDD = hbaseRDD
    //      .map(x => x._2)
    //      .map { result =>
    //        (result.getRow, result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("uid_")), result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("couponid_")))
    //      }.map(row => (new String(row._1), new String(row._2), new String(row._3)))
    //      .collect()
    //      .foreach(r => (println("rowKey:" + r._1 + ", name:" + r._2 + ", age:" + r._3)))

    val dataRDD = hbaseRDD
      .map(x => x._2)
      .map { result =>
        //RowFactory.create(
        (Bytes.toString(result.getRow), Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("couponid_"))),
          Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("couponname_"))),
          Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("etime_"))),
          Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("sendch_"))),
          Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("status_"))),
          Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("stime_"))),
          Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("uid_"))))
      }
      .map(x => {
        RowFactory.create(
          x._1,
          x._2,
          x._3,
          if (x._4.length() > 0) { x._4.toLong.asInstanceOf[Long] } else { 0L.asInstanceOf[Long] },
          x._5,
          x._6,
          if (x._7.length() > 0) { x._7.toLong.asInstanceOf[Long] } else { 0L.asInstanceOf[Long] },
          x._8)
      })

    val structFields = new ArrayList[StructField]();
    structFields.add(DataTypes.createStructField("rowkeyid", DataTypes.StringType, true))
    structFields.add(DataTypes.createStructField("couponid_", DataTypes.StringType, false))
    structFields.add(DataTypes.createStructField("couponname_", DataTypes.StringType, false))
    structFields.add(DataTypes.createStructField("etime_", DataTypes.LongType, false))
    structFields.add(DataTypes.createStructField("sendch_", DataTypes.StringType, false))
    structFields.add(DataTypes.createStructField("status_", DataTypes.StringType, false))
    structFields.add(DataTypes.createStructField("stime_", DataTypes.LongType, false))
    structFields.add(DataTypes.createStructField("uid_", DataTypes.StringType, false))

    val schema = DataTypes.createStructType(structFields)
    val df = sparkSession.createDataFrame(dataRDD, schema)

    println("df数量:" + df.count())
    df.show()

    sc.stop()
  }
}