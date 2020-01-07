package com.fyc.toDB

import com.fyc.utils.JDBCUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class Init {

  private val url = DBConf.url
  private val prop = DBConf.prop

  private def app_comm_pri(spark: SparkSession)={

    val tbName = "wp_comm_pri"
    JDBCUtils.dropAll(tbName)
    val df: DataFrame = spark.sql("select index_id,index_name,value,unit,data_time,area_name from zhjg.app_comm_pri_org")
    df.na.drop.write
      .mode(SaveMode.Append)
      .jdbc(url,tbName,prop)
  }
  private def app_comm_pri_org(spark: SparkSession)={
    val tbName = "wp_comm_pri_org"
    JDBCUtils.dropAll(tbName)
    val df: DataFrame = spark.sql("select index_id,index_name,value,unit,data_time,area_name from zhjg.app_comm_pri_org")
    df.na.drop.write
      .mode(SaveMode.Append)
      .jdbc(url,tbName,prop)
  }
  private def app_base_index_info(spark: SparkSession)={
    val tbName = "wp_base_index_info"
    JDBCUtils.dropAll(tbName)
    val df: DataFrame = spark.sql("select * from zhjg.app_base_index_info")
    df.na.drop.write
      .mode(SaveMode.Append)
      .jdbc(url,tbName,prop)
  }
  private def app_base_index_val(spark: SparkSession)={
    val tbName = "wp_base_index_val"
    JDBCUtils.dropAll(tbName)
    val df: DataFrame = spark.sql("select index_id,comm_id,index_name,index_type,area_name,frequence,unit,source_name,value,date data_time from zhjg.app_base_index_val")
    df.show()
    df.na.drop.write
      .mode(SaveMode.Append)
      .jdbc(url,tbName,prop)
  }
  private def app_macro_index_info(spark: SparkSession)={
    val tbName = "wp_macro_index_info"
    JDBCUtils.dropAll(tbName)
    val df: DataFrame = spark.sql("select * from zhjg.app_macro_index_info")
    df.na.drop.write
      .mode(SaveMode.Append)
      .jdbc(url,tbName,prop)
  }
  private def app_macro_index_val(spark: SparkSession)={
    val tbName = "wp_macro_index_info"
    JDBCUtils.dropAll(tbName)
    val df: DataFrame = spark.sql("select * from zhjg.app_macro_index_val")
    df.na.drop.write
      .mode(SaveMode.Append)
      .jdbc(url,tbName,prop)
  }
  def init(spark: SparkSession)={
    app_base_index_info(spark)
    app_base_index_val(spark)
    app_macro_index_info(spark)
    //    app_macro_index_val(spark)
    app_comm_pri(spark)
    app_comm_pri_org(spark)
  }
}
object Init extends Init

