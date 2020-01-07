package com.fyc.toDB

import java.util.Properties

import com.fyc.config.globalConfUtils
import com.fyc.utils.{JDBCUtils, dateUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DateType, FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

object Freq2DB {


  private val url=DBConf.url
  private val prop: Properties = DBConf.prop
  private var date = ""
  private var s= ""
  def run(spark: SparkSession,tuple: (DataFrame, DataFrame),freq:Int)={
    if (freq==globalConfUtils.day){
      date = dateUtils.getYesterday
      s="日"
      val df: DataFrame = app_base_index_val(spark,tuple._1)
      wp_comm_pri(spark,df)
    }else if (freq==globalConfUtils.week){
      date = dateUtils.getLastWeekN(5)
      s="周"
      app_base_index_val(spark,tuple._1)
    }else if (freq==globalConfUtils.mouth){
      date = dateUtils.getLastMonthEnd
      s="月"
      app_base_index_val(spark,tuple._1)
    }


  }

  private def app_base_index_val(spark: SparkSession,df:DataFrame)={
    val tbName = "wp_base_index_val"

    val indexs: DataFrame = spark.sql("select index_id,comm_id,index_name,index_type,area_name,frequence,unit,source_name from zhjg.app_base_index_info where frequence ='"+s+"'")
    val rdd: RDD[Row] = indexs.rdd.map(
      row =>
        Row(row(0), row(1), row(2), row(3), row(4), row(5), row(6), row(7), 0f, dateUtils.toSqlDate(date))
    )
    val structType: StructType = StructType(List(
      StructField("index_id", IntegerType, true),
      StructField("comm_id", IntegerType, false),
      StructField("index_name", StringType, true),
      StructField("index_type", StringType, true),
      StructField("area_name", StringType, true),
      StructField("frequence", StringType, true),
      StructField("unit", StringType, true),
      StructField("source_name", StringType, true),
      StructField("value", FloatType, true),
      StructField("date", DateType, true)
    ))
    val temp: DataFrame = spark.createDataFrame(rdd, structType).unionAll(df.drop("frequence_id","year","month","day")).toDF()
    temp.createOrReplaceTempView("data")
    temp.show(10000)
    df.show(10000)
    val result: DataFrame = spark.sql("select index_id,first(comm_id) comm_id,first(index_name) index_name,first(index_type) index_type,first(area_name) area_name,first(frequence) frequence,first(unit) unit,first(source_name) source_name,sum(value) value,first(date) data_time from data group by index_id")


    JDBCUtils.dropByday(tbName,date)

    result
      .write
      .mode(SaveMode.Append)
      .jdbc(url,tbName,prop)

    result
  }
  private def app_base_macro_val(spark: SparkSession,df:DataFrame)={
    val tbName = "app_base_macro_val"

    JDBCUtils.dropByday(tbName,date)

    df.na.drop.write
      .mode(SaveMode.Append)
      .jdbc(url,tbName,prop)
  }
  private def wp_comm_pri(spark: SparkSession,df:DataFrame)={
    val tbName = "wp_comm_pri"
    JDBCUtils.dropByday(tbName,date)

    df.createOrReplaceTempView("data")
    spark.sql("select index_id,index_name,value,unit,data_time,area_name from data where area_name='中国' and index_type ='价格'")
      .write
      .mode(SaveMode.Append)
      .jdbc(url,tbName,prop)
  }
  private def wp_comm_pri_org(spark: SparkSession)={
    val tbName = "wp_comm_pri_org"
    JDBCUtils.dropAll(tbName)
    val df: DataFrame = spark.sql("select index_id,index_name,value,unit,data_time,area_name from zhjg.app_comm_pri_org")
    df.na.drop.write
      .mode(SaveMode.Append)
      .jdbc(url,tbName,prop)
  }


}

