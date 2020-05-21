package com.test.kudu

import org.apache.spark.sql.SparkSession
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.kudu.client.CreateTableOptions
import scala.collection.JavaConverters._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.Random

//@SerialVersionUID(-6743567631108323096L)
case class Cust(id: Long, name: String)
object KuduTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    //        sparkConf.set("spark.driver.userClassPathFirst", "true")
    //        sparkConf.set("spark.executor.userClassPathFirst", "true")
    sparkConf.setAppName("KuduTest")
    sparkConf.set("spark.ui.port", new Random(1024).nextInt(65535).toString())

    val spark = SparkSession.builder()
      .appName("KuduTest")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse/spark-warehouse")
      .config(sparkConf)
      .master("local[*]")
      .getOrCreate()

    //           sparkConf.set("spark.driver.userClassPathFirst", "true")
    //        sparkConf.set("spark.executor.userClassPathFirst", "true")

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    //    val custs = Array(
    //      Cust(7, "龙lola"),
    //      Cust(8, "janeni"),
    //      Cust(9, "jordanruan"),
    //      Cust(10, "enzoli"),
    //      Cust(11, "laurahan"))
    //    import spark.implicits._
    //    val custsRDD = sc.parallelize(custs)
    //    val custDF = custsRDD.toDF()

    //创建kudu客户端
    val kuduContext = new KuduContext("172.16.8.137:7051", sc) //spark.sparkContext

    //    1.spark创建kudu表
    val kuduTableSchema = createReg()

    kuduContext.deleteTable("db_user_tags.t_user_reg")

    kuduContext.createTable("db_user_tags.t_user_reg", kuduTableSchema, Seq("rowkeyid"), new CreateTableOptions()
      .setNumReplicas(1) //设置副本数量,<3台的集群最好是设置为1 ,不然可能会报错
      //    .addHashPartitions(List("id").asJava, 2)
      .addHashPartitions(List("rowkeyid").asJava, 2)) //设置分区 数据分到几台机器上   //List要这样用需要  import scala.collection.JavaConverters._

    //2.添加数据
    //    kuduContext.upsertRows(custDF, "spark_kudu_test")
    //    kuduContext.insertRows(custDF, "spark_kudu_test")

    sc.stop()
  }

  def createReg(): StructType = {
    val kuduTableSchema = StructType(
      StructField("rowkeyid", dataType = StringType, nullable = false) ::
        StructField("uid_", dataType = StringType, nullable = false) ::
        StructField("reg_timestamp_", dataType = LongType, nullable = true) ::
        StructField("plat_", dataType = StringType, nullable = true) ::
        StructField("channel_", dataType = LongType, nullable = true) ::
        StructField("app_", dataType = StringType, nullable = true) :: Nil)
    kuduTableSchema
  }
  def createTraceUser(): StructType = {
    val kuduTableSchema = StructType(
      StructField("rowkeyid", dataType = StringType, nullable = false) ::
        StructField("uid_", dataType = StringType, nullable = false) ::
        StructField("investment_amount_", dataType = StringType, nullable = true) ::
        StructField("yearly_income_", dataType = StringType, nullable = true) ::
        StructField("investment_experience_", dataType = StringType, nullable = true) ::
        StructField("investment_time_", dataType = StringType, nullable = true) :: Nil)
    kuduTableSchema
  }
  def createUserAccount(): StructType = {
    val kuduTableSchema = StructType(
      StructField("rowkeyid", dataType = StringType, nullable = false) ::
        StructField("uid_", dataType = StringType, nullable = false) ::
        StructField("trader_company_", dataType = StringType, nullable = true) ::
        StructField("open_success_", dataType = StringType, nullable = true) ::
        StructField("amount_type_", dataType = StringType, nullable = true) :: Nil)
    kuduTableSchema
  }
  def createTgFollow(): StructType = {
    val kuduTableSchema = StructType(
      StructField("rowkeyid", dataType = StringType, nullable = false) ::
        StructField("uid_", dataType = StringType, nullable = false) ::
        StructField("tgName_", dataType = StringType, nullable = true) ::
        StructField("follow_time_", dataType = LongType, nullable = true) :: Nil)
    kuduTableSchema
  }
  def createUserLogin(): StructType = {
    val kuduTableSchema = StructType(
      StructField("rowkeyid", dataType = StringType, nullable = false) ::
        StructField("uid_", dataType = StringType, nullable = false) ::
        StructField("app", dataType = StringType, nullable = true) ::
        StructField("plat", dataType = StringType, nullable = true) ::
        StructField("channel", dataType = LongType, nullable = true) ::
        StructField("login_time", dataType = LongType, nullable = true) ::
        StructField("reg_time", dataType = LongType, nullable = true) :: Nil)
    kuduTableSchema
  }
  def createCouponOrder(): StructType = {
    val kuduTableSchema = StructType(
      StructField("rowkeyid", dataType = StringType, nullable = false) ::
        StructField("uid_", dataType = StringType, nullable = false) ::
        StructField("couponid_", dataType = StringType, nullable = true) ::
        StructField("sendch_", dataType = StringType, nullable = true) ::
        StructField("status_", dataType = StringType, nullable = true) ::
        StructField("etime_", dataType = LongType, nullable = true) ::
        StructField("couponname_", dataType = StringType, nullable = true) ::
        StructField("stime_", dataType = LongType, nullable = true) :: Nil)
    kuduTableSchema
  }

  def createTable2(): StructType = {
    val kuduTableSchema = StructType(
      StructField("id", dataType = LongType, nullable = false) ::
        StructField("name", StringType, true) :: Nil)
    kuduTableSchema
  }
  def createTableOrder(): StructType = {
    val kuduTableSchema = StructType(
      StructField("rowkeyid", dataType = StringType, nullable = false) ::
        StructField("channel_", dataType = LongType, nullable = true) ::
        StructField("ctime_", dataType = LongType, nullable = true) ::
        StructField("endtime_", dataType = LongType, nullable = true) ::
        StructField("groupid_", dataType = StringType, nullable = true) ::
        StructField("moduleid_", dataType = LongType, nullable = true) ::
        StructField("payeename_", dataType = StringType, nullable = true) ::
        StructField("paytotal_", dataType = LongType, nullable = true) ::
        StructField("product_", dataType = StringType, nullable = true) ::
        StructField("productid_", dataType = StringType, nullable = true) ::
        StructField("starttime_", dataType = LongType, nullable = true) ::
        StructField("status_", dataType = StringType, nullable = true) ::
        StructField("uid_", StringType, false) :: Nil)
    kuduTableSchema
  }

}