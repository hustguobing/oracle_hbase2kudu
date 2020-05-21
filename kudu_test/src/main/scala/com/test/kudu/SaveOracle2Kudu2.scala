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

case class stk(stk_uni_code: Int, end_date: String, trade_date: String, close_price: Float,
               pre_close_price: Float, open_price: Float, high_price: Float, low_price: Float,
               close_price_re: Float, pre_close_price_re: Float, open_price_re: Float, high_price_re: Float,
               low_price_re: Float, trade_vol: Long, trade_amut: Float, trade_num: Int,
               vibr_range: Float, turnover_rate: Float, rise_drop: Float, rise_drop_range: Float,
               rise_drop_re: Float, rise_drop_range_re: Float, stk_per: Float, price_bookv_ratio: Float,
               stk_cir_value: Float, stk_tot_value: Float, stk_per_ttm: Float, stk_pc: Float,
               stk_pc_ttm: Float, stk_poc: Float, stk_poc_ttm: Float, stk_ps: Float,
               stk_ps_ttm: Float, stk_cir_value_b: Float, stk_tot_value_b: Float, beta: Float,
               log_ret: Float, list_day: Int, rise_day: Int, drop_day: Int,
               log_close_price: Float, close_price_rmb: Float, is_lmt_up: Int, is_int_lmt_up: Int,
               peg_lyr: Float, stk_per_d_ttm: Float, stk_per_mov: Float, dvd_rat: Float,
               cash_dvd_ttm: Float, dvd_rat_ttm: Float, cash_dvd_lfy: Float, isvalid: Int,
               createtime: String, updatetime: String)

object SaveOracle2Kudu2 {
  def main(args: Array[String]): Unit = {

    //    val year = "19901211"
    val year = args(0)
    val spark = SparkSession
      .builder()
      //      .master("local[*]")
      .appName("SaveOracle2Kudu2:" + year)
      //          .config("spark.driver.cores", "1")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse/spark-warehouse")
      .getOrCreate()

    val sc = spark.sparkContext
    //
    //    val jdbcDF = spark.read
    //      .format("jdbc")
    //      .option("url", "jdbc:oracle:thin:@172.16.8.31:1521:upwhdb")
    //      .option("dbtable", "upcenter.stk_basic_price_mid")
    //      .option("user", "haitao_read")
    //      .option("password", "haitao")
    //      .option("driver", "oracle.jdbc.driver.OracleDriver")
    //      .option("query", s"select * from upcenter.stk_basic_price_mid where to_char(end_date,'yyyy')='${year}'")
    //      //          .option("numPartitions", 8) //numPartitlons:Int,#分区的个数
    //      //          .option("partitionColumn", "STK_UNI_CODE")
    //      //          //      .option("fetchSize", "1000")
    //      //          .option("lowerBound", 2010000003)
    //      //          .option("upperBound", max)
    //      .load();
    //
    //    //    jdbcDF.show()
    //
    //    println("jdbcDF数量:" + jdbcDF.count())
    //    //创建kudu客户端
    //    val kuduContext = new KuduContext("172.16.8.137:7051", sc) //spark.sparkContext
    //
    //    kuduContext.upsertRows(jdbcDF.repartition(32), "impala::default.stk_basic_price_mid")

    val sql = s"""select * from upcenter.stk_basic_price_mid  where (1 = 1 or ?=?) and to_char(end_date,'yyyyMMdd')='${year}'"""
    val rdd = new JdbcRDD(
      sc,
      getHisOracleConnection _,
      sql,
      0L,
      1,
      1,
      r => (stk(
        r.getInt("stk_uni_code"), r.getString("end_date"), r.getString("trade_date"), r.getFloat("close_price"),
        r.getFloat("pre_close_price"), r.getFloat("open_price"), r.getFloat("high_price"), r.getFloat("low_price"),
        r.getFloat("close_price_re"), r.getFloat("pre_close_price_re"), r.getFloat("open_price_re"), r.getFloat("high_price_re"),
        r.getFloat("low_price_re"), r.getLong("trade_vol"), r.getFloat("trade_amut"), r.getInt("trade_num"),
        r.getFloat("vibr_range"), r.getFloat("turnover_rate"), r.getFloat("rise_drop"), r.getFloat("rise_drop_range"),
        r.getFloat("rise_drop_re"), r.getFloat("rise_drop_range_re"), r.getFloat("stk_per"), r.getFloat("price_bookv_ratio"),
        r.getFloat("stk_cir_value"), r.getFloat("stk_tot_value"), r.getFloat("stk_per_ttm"), r.getFloat("stk_pc"),
        r.getFloat("stk_pc_ttm"), r.getFloat("stk_poc"), r.getFloat("stk_poc_ttm"), r.getFloat("stk_ps"),
        r.getFloat("stk_ps_ttm"), r.getFloat("stk_cir_value_b"), r.getFloat("stk_tot_value_b"), r.getFloat("beta"),
        r.getFloat("log_ret"), r.getInt("list_day"), r.getInt("rise_day"), r.getInt("drop_day"),
        r.getFloat("log_close_price"), r.getFloat("close_price_rmb"), r.getInt("is_lmt_up"), r.getInt("is_int_lmt_up"),
        r.getFloat("peg_lyr"), r.getFloat("stk_per_d_ttm"), r.getFloat("stk_per_mov"), r.getFloat("dvd_rat"),
        r.getFloat("cash_dvd_ttm"), r.getFloat("dvd_rat_ttm"), r.getFloat("cash_dvd_lfy"), r.getInt("isvalid"),
        r.getString("createtime"), r.getString("updatetime"))))

    println(rdd.count())
    if (rdd.count() == 0) {
      sc.stop()
    }
    //      val arr = rdd
    //    .filter(x => x.stk_uni_code == "2010000007").collect()

    //    for (i <- arr) {
    //      println(i)
    //    }
    import spark.implicits._
    val arr = rdd.repartition(45)
      .toDF()

    println("本次插入数目:" + arr.count())

    //    val kuduContext = new KuduContext("172.16.8.137:7051", sc) //spark.sparkContext
    //
    //    kuduContext.upsertRows(arr, "impala::default.stk_basic_price_mid")

    import org.apache.kudu.spark.kudu._
    import org.apache.kudu.client._
    import collection.JavaConverters._
    arr.write
      .option("kudu.master", "172.16.8.137:7051")
      .option("kudu.table", "impala::default.stk_basic_price_mid")
      .mode("append")
      //      .mode("overwrite")  //kudu在这里不支持overwrite,支持append
      .kudu

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