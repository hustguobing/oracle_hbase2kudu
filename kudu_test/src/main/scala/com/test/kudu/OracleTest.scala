package com.test.kudu

import java.util.Properties
import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ Row, SparkSession, Dataset }

object OracleTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("OracleTest")
      .config("spark.driver.cores", "1")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse/spark-warehouse")
      .getOrCreate()

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:oracle:thin:@172.16.8.31:1521:upwhdb")
      .option("dbtable", "upcenter.stk_basic_price_mid")
      .option("user", "haitao_read")
      .option("password", "haitao")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load();
    jdbcDF.show()
    
    
    
    
    
    
    
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
    //        sc.stop()

  }
}