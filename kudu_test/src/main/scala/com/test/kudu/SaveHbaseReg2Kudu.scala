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
import org.apache.kudu.client.KuduClient
import org.apache.kudu.client.SessionConfiguration

object SaveHbaseReg2Kudu {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("SaveHbaseReg2Kudu")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse/spark-warehouse")
      .config("spark.worker.timeout", "5000")
      //      .config("spark.cores.max", "10")
      .config("spark.rpc.askTimeout", "6000s")
      .config("spark.network.timeout", "6000s")
      //      .config("spark.task.maxFailures", "1")
      //      .config("spark.speculationfalse", "false")
      .config("spark.driver.allowMultipleContexts", "true")
      //      .master("local[*]")
      .getOrCreate()
    val sc = sparkSession.sparkContext

    //读取hbase数据
    val tableName = "db_user_tags_t_user_reg"
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
          if (result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("uid_")) == null) { "" }
          else { Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("uid_"))) },
          if (result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("reg_timestamp_")) == null) { "" } else { Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("reg_timestamp_"))) },
          if (result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("plat_")) == null) { "" } else { Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("plat_"))) },
          if (result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("channel_")) == null) { "" } else { Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("channel_"))) },
          if (result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("app_")) == null) { "" } else { Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("app_"))) })
      }
      .map(x => {
        RowFactory.create(
          x._1,
          x._2,
          if (x._3.length() > 0) { x._3.toLong.asInstanceOf[Long] } else { 0L.asInstanceOf[Long] },
          x._4,
          if (x._5.length() > 0) { x._5.toLong.asInstanceOf[Long] } else { 0L.asInstanceOf[Long] },
          x._6)
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
    structFields.add(DataTypes.createStructField("uid_", DataTypes.StringType, false))
    structFields.add(DataTypes.createStructField("reg_timestamp_", DataTypes.LongType, false))
    structFields.add(DataTypes.createStructField("plat_", DataTypes.StringType, false))
    structFields.add(DataTypes.createStructField("channel_", DataTypes.LongType, false))
    structFields.add(DataTypes.createStructField("app_", DataTypes.StringType, false))

    val schema = DataTypes.createStructType(structFields)
    val df = sparkSession.createDataFrame(dataRDD, schema)

    println("df数量:" + df.count())
    //    df.show()

    //创建kudu客户端
    val kuduContext = new KuduContext("172.16.8.137:7051", sc) //spark.sparkContext

    kuduContext.upsertRows(df.repartition(50), "db_user_tags.t_user_reg")

    //        val client = new KuduClient.KuduClientBuilder("172.16.8.137")
    //          .defaultAdminOperationTimeoutMs(60000).defaultSocketReadTimeoutMs(60000)
    //          .defaultOperationTimeoutMs(60000)
    //          .build();
    //        // 创建写session,kudu必须通过session写入
    //        val session = client.newSession();
    //        // 采取Flush方式 手动刷新
    //        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    //        session.setMutationBufferSpace(600000);
    //        df.foreach(x =>{
    //
    //        })

    sc.stop()
  }
}