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
import java.math.BigInteger
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.RowFactory
import java.util.ArrayList
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataTypes
import org.apache.kudu.spark.kudu.KuduContext
import java.lang.Long

object SaveHbaseOrder2Kudu {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("SaveHbaseOrder2Kudu")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse/spark-warehouse")
      //      .master("local[*]")
      .getOrCreate()
    val sc = sparkSession.sparkContext

    //读取hbase数据
    val tableName = "db_user_tags_t_user_order_info"
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "172.16.9.102,172.16.9.102,172.16.9.102")
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)

    val scan = new Scan()
    //    scan.setStartRow(Bytes.toBytes("1150201041934315-05010066"))
    //    scan.setStopRow(Bytes.toBytes("1150201041934315-05010066"))
    scan.addFamily(Bytes.toBytes("f1"))
    val proto = ProtobufUtil.toScan(scan)
    val scanToString = new String(Base64.getEncoder.encode(proto.toByteArray()))
    hbaseConf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.SCAN, scanToString)

    //读取数据并转化成rdd TableInputFormat是org.apache.hadoop.hbase.mapreduce包下的
    val hbaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    val dataRDD = hbaseRDD
      .map(x => x._2)
      .map { result =>
        //        RowFactory.create(
        (Bytes.toString(result.getRow),
          if (result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("channel_")) == null) { "" }
          else { Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("channel_"))) },
          if (result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("ctime_")) == null) { "" } else { Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("ctime_"))) },
          if (result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("endtime_")) == null) { "" } else { Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("endtime_"))) },
          if (result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("groupid_")) == null) { "" } else { Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("groupid_"))) },
          if (result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("moduleid_")) == null) { "" } else { Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("moduleid_"))) },
          if (result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("payeename_")) == null) { "" } else { Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("payeename_"))) },
          if (result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("paytotal_")) == null) { "" } else { Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("paytotal_"))) },
          if (result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("product_")) == null) { "" } else { Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("product_"))) },
          if (result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("productid_")) == null) { "" } else { Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("productid_"))) },
          if (result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("starttime_")) == null) { "" } else { Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("starttime_"))) },
          if (result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("status_")) == null) { "" } else { Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("status_"))) },
          if (result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("uid_")) == null) { "" } else { Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("uid_"))) })
      }
      .map(x => {
        RowFactory.create(
          x._1,
          if (x._2.length() > 0) { x._2.toLong.asInstanceOf[Long] } else { 0L.asInstanceOf[Long] },
          if (x._3.length() > 0) { x._3.toLong.asInstanceOf[Long] } else { 0L.asInstanceOf[Long] },
          if (x._4.length() > 0) { x._4.toLong.asInstanceOf[Long] } else { 0L.asInstanceOf[Long] },
          x._5,
          if (x._6.length() > 0) { x._6.toLong.asInstanceOf[Long] } else { 0L.asInstanceOf[Long] },
          x._7,
          if (x._8.length() > 0) { x._8.toLong.asInstanceOf[Long] } else { 0L.asInstanceOf[Long] },
          x._9, x._10,
          if (x._11.length() > 0) { x._11.toLong.asInstanceOf[Long] } else { 0L.asInstanceOf[Long] },
          x._12, x._13)
      })
      //.asInstanceOf[Long]
      .persist(StorageLevel.DISK_ONLY)
    //    println("dataRDD总数为:" + dataRDD.count())
    //
    //    for (i <- dataRDD.collect()) {
    //      println(i)
    //    }

    //df 模式读数据    需加上  RowFactory.create(
    val structFields = new ArrayList[StructField]();
    structFields.add(DataTypes.createStructField("rowkeyid", DataTypes.StringType, true))
    structFields.add(DataTypes.createStructField("channel_", DataTypes.LongType, false))
    structFields.add(DataTypes.createStructField("ctime_", DataTypes.LongType, false))
    structFields.add(DataTypes.createStructField("endtime_", DataTypes.LongType, false))
    structFields.add(DataTypes.createStructField("groupid_", DataTypes.StringType, false))
    structFields.add(DataTypes.createStructField("moduleid_", DataTypes.LongType, false))
    structFields.add(DataTypes.createStructField("payeename_", DataTypes.StringType, false))
    structFields.add(DataTypes.createStructField("paytotal_", DataTypes.LongType, false))
    structFields.add(DataTypes.createStructField("product_", DataTypes.StringType, false))
    structFields.add(DataTypes.createStructField("productid_", DataTypes.StringType, false))
    structFields.add(DataTypes.createStructField("starttime_", DataTypes.LongType, false))
    structFields.add(DataTypes.createStructField("status_", DataTypes.StringType, false))
    structFields.add(DataTypes.createStructField("uid_", DataTypes.StringType, true))

    val schema = DataTypes.createStructType(structFields)
    val df = sparkSession.createDataFrame(dataRDD, schema)

    println("df数量:" + df.count())
    //    df.show()

    //创建kudu客户端
    val kuduContext = new KuduContext("172.16.8.137:7051", sc) //spark.sparkContext
    kuduContext.upsertRows(df.repartition(50), "db_user_tags.t_user_order_info")

    sc.stop()
  }
}