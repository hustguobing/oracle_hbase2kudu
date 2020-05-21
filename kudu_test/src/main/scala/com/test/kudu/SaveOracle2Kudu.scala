package com.test.kudu

import java.util.Properties
import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ Row, SparkSession, Dataset }
import org.apache.kudu.spark.kudu.KuduContext
import scala.collection.mutable.ArrayBuffer
import java.sql.Connection
import java.sql.DriverManager
import org.apache.spark.rdd.JdbcRDD

object SaveOracle2Kudu {
  def main(args: Array[String]): Unit = {

    val year = args(0)
    val spark = SparkSession
      .builder()
      //      .master("local[*]")
      .appName("SaveOracle2Kudu")
      //          .config("spark.driver.cores", "1")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse/spark-warehouse")
      .getOrCreate()

    val sc = spark.sparkContext

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:oracle:thin:@172.16.8.31:1521:upwhdb")
      .option("dbtable", "upcenter.stk_basic_price_mid")
      .option("user", "haitao_read")
      .option("password", "haitao")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("query", s"select * from upcenter.stk_basic_price_mid where to_char(end_date,'yyyyMMdd')='${year}'")
      .option("numPartitions", 8) //numPartitlons:Int,#分区的个数
      .option("partitionColumn", "STK_UNI_CODE")
      //      .option("fetchSize", "1000")
      .option("lowerBound", 2010000003)
      .option("upperBound", 2010019875)
      .load();

    jdbcDF.show()

    println("jdbcDF数量:" + jdbcDF.count())
    //创建kudu客户端
    val kuduContext = new KuduContext("172.16.8.137:7051", sc) //spark.sparkContext

    kuduContext.upsertRows(jdbcDF.repartition(32), "impala::default.stk_basic_price_mid")

    //    val sql = s"""select * from upcenter.stk_basic_price_mid  where (1 = 1 or ?=?) and to_char(end_date,'yyyy')='${year}'"""
    //    val rdd = new JdbcRDD(
    //      sc,
    //      getHisOracleConnection _,
    //      sql,
    //      0L,
    //      1,
    //      1,
    //      r => (
    //        r.getInt("stk_uni_code"), r.getString("end_date"), r.getString("trade_date"), r.getFloat("close_price"),
    //        r.getFloat("pre_close_price"), r.getFloat("open_price"), r.getFloat("high_price"), r.getFloat("low_price"),
    //        r.getFloat("close_price_re"), r.getFloat("pre_close_price_re"), r.getFloat("open_price_re"), r.getFloat("high_price_re"),
    //        r.getFloat("low_price_re"), r.getLong("trade_vol"), r.getFloat("trade_amut"), r.getInt("trade_num"),
    //        r.getInt("stk_uni_code"), r.getString("end_date"), r.getString("trade_date"), r.getFloat("close_price"),
    //        r.getInt("stk_uni_code"), r.getString("end_date"), r.getString("trade_date"), r.getFloat("close_price"),
    //        r.getInt("stk_uni_code"), r.getString("end_date"), r.getString("trade_date"), r.getFloat("close_price"),
    //        r.getInt("stk_uni_code"), r.getString("end_date"), r.getString("trade_date"), r.getFloat("close_price"),
    //        r.getInt("stk_uni_code"), r.getString("end_date"), r.getString("trade_date"), r.getFloat("close_price"),
    //        r.getInt("stk_uni_code"), r.getString("end_date"), r.getString("trade_date"), r.getFloat("close_price"),
    //        r.getInt("stk_uni_code"), r.getString("end_date"), r.getString("trade_date"), r.getFloat("close_price"),
    //        r.getInt("stk_uni_code"), r.getString("end_date"), r.getString("trade_date"), r.getFloat("close_price"),
    //        r.getInt("stk_uni_code"), r.getString("end_date"), r.getString("trade_date"), r.getFloat("close_price"),
    //        r.getInt("stk_uni_code"), r.getString("end_date"), r.getString("trade_date"), r.getFloat("close_price")
    //        ))

    //    import org.apache.spark.sql.Dataset
    //    jdbcDF.createOrReplaceTempView("TEST_TABLE")
    //    val result = spark.sql("SELECT * FROM TEST_TABLE")
    //    result.show()

    //        val sparlkConf = new SparkConf().setAppName("OracleTest").setMaster("local[*]")
    //          .set("spark.sql.warehouse.dir", "/user/hive/warehouse/spark-warehouse")
    //        val sc = new SparkContext(sparlkConf)
    //        val sqlContext = new SQLContext(sc)
    //        val property = new Properties()
    //        val url = "jdbc:oracle:thin:@//172.16.8.31:1521/upwhdb"
    //        property.put("driver", "oracle.jdbc.driver.OracleDriver")
    //        property.put("user", "haitao_read")
    //        property.put("password", "haitao")
    //        val jdbcDF = sqlContext.read.jdbc(url, "upcenter.stk_basic_price_mid", property)
    //        jdbcDF.registerTempTable("records")
    //        sqlContext.sql("select * from upcenter.stk_basic_price_mid limit 1").collect().foreach(println)
    //

    sc.stop()

  }

  def getHisOracleConnection: Connection = {
    val url = "jdbc:oracle:thin:@//" + "172.16.8.31" + ":" + "1521" + "/" + "upwhdb"
    val driver = "oracle.jdbc.driver.OracleDriver"
    Class.forName(driver)
    val connection = DriverManager.getConnection(url, "haitao_read", "haitao")
    connection
  }

}