package com.fyc.concordance

import org.apache.spark.sql.{DataFrame, SparkSession}

class app {


  private def base_info(spark: SparkSession)={
    val sql = "select a.index_id,a.rpst_comm_id comm_id,a.index_name,a.index_type,a.unit,a.index_used,a.frequence,a.area_name,a.source_name,a.index_flag,a.index_describe from zhjg.wp_base_index_info a where a.index_used <> '存储'"
    val app_base_index_info: DataFrame = spark.sql(sql)
    app_base_index_info.coalesce(1).write.format("hive").mode(saveMode = "overwrite")
      .saveAsTable("zhjg.app_base_index_info")
  }
  private def base_val(spark: SparkSession)={
    val sql = "SELECT b.index_id,b.comm_id,b.index_name,b.index_type,b.area_name,b.frequence,b.unit,b.source_name,a.value,a.`date`,a.frequence_id,a.year,a.month,a.day FROM zhjg.wp_base_index_val a LEFT JOIN zhjg.app_base_index_info b on a.index_id = b.index_id"
    val app_base_index_val: DataFrame = spark.sql(sql).na.drop()
    app_base_index_val.coalesce(1).write.format("hive").mode(saveMode = "overwrite")
      .partitionBy("frequence_id","year","month","day")
      .saveAsTable("zhjg.app_base_index_val")

  }
  private def macro_info(spark: SparkSession)={
    val sql = "SELECT a.index_id,a.index_name,a.area_name,a.unit,a.frequence,a.index_type,a.source_name,a.index_status index_flag from zhjg.wp_macro_index_info a"
    val app_macro_index_info: DataFrame = spark.sql(sql)
    app_macro_index_info.coalesce(1).write.format("hive").mode(saveMode = "overwrite")
      .saveAsTable("zhjg.app_macro_index_info")
  }
  private def macro_val(spark: SparkSession)={
    val sql = "SELECT b.area_id,b.index_name,b.index_type,b.frequence,b.unit,b.source_name,a.value,a.`date` from zhjg.wp_macro_index_val a LEFT JOIN zhjg.wp_macro_index_info b on a.index_id = b.index_id"
    val app_macro_index_val: DataFrame = spark.sql(sql)
    app_macro_index_val.coalesce(1).write.format("hive").mode(saveMode = "overwrite")
      .saveAsTable("zhjg.app_macro_index_val")
  }
  private def comm_pri(spark: SparkSession)={
    val sql = "SELECT a.index_id,a.index_name,a.value,a.unit,a.`date` data_time,a.area_name,a.frequence_id,a.year,a.month,a.day from zhjg.app_base_index_val a WHERE a.frequence='日' AND (a.area_name ='全国' or a.area_name = '中国')"
    val app_comm_pri: DataFrame = spark.sql(sql)
    app_comm_pri.coalesce(1).write.format("hive").mode(saveMode = "overwrite")
      .partitionBy("frequence_id","year","month","day")
      .saveAsTable("zhjg.app_comm_pri")

  }
  private def comm_pri_org(spark: SparkSession)={
  }
  private def ascii(spark: SparkSession)={

  }
  private def comm_total(spark: SparkSession)={

  }
  def init(spark: SparkSession)={
    base_info(spark)
    base_val(spark)
    macro_info(spark)
//    macro_val(spark)
    comm_pri(spark)
  }
}
object app extends app

