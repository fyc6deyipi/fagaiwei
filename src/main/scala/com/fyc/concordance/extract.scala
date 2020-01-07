package com.fyc.concordance

import com.fyc.config.globalConfUtils
import com.fyc.utils.dateUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object extract {


  def byDay(df:DataFrame,spark: SparkSession)={
    val freq =globalConfUtils.day
    val appBaseVal: DataFrame = toAppBaseVal(spark,df,freq)
    val appCommPri: DataFrame = toAppCommPri(spark,appBaseVal,freq)
    (appBaseVal,appCommPri)
  }

  def byFreq(df:DataFrame,spark: SparkSession,freq:Int)={
    val appBaseVal: DataFrame = toAppBaseVal(spark,df,freq)
    val appMacroVal: DataFrame =null
    if (freq==globalConfUtils.day){
       toAppCommPri(spark,appBaseVal,freq)
    }
    (appBaseVal,appMacroVal)
  }




  private def toAppBaseVal(spark: SparkSession,df:DataFrame,freq:Int)={
    var s=""
    if(freq==globalConfUtils.day){
      s="日"
    }else if(freq==globalConfUtils.week){
      s="周"
    }else if(freq==globalConfUtils.mouth){
      s="月"
    }

    val tbName = "zhjg.app_base_index_val"
    dropPartition(spark,tbName,freq)
    df.createOrReplaceTempView("data")
    val sql = "SELECT b.index_id,b.comm_id,b.index_name,b.index_type,b.area_name,b.frequence,b.unit,b.source_name,a.value,a.`date`,a.frequence_id,a.year,a.month,a.day FROM data a left JOIN (select * from zhjg.app_base_index_info where frequence = '"+s+"') b on a.index_id = b.index_id"
    val data: DataFrame = spark.sql(sql).na.drop()
    data.show()
    data.coalesce(1).write.format("hive").mode(saveMode = "append")
      .partitionBy("frequence_id","year","month","day")
      .saveAsTable(tbName)
    data
  }
  private def toAppCommPri(spark: SparkSession,df:DataFrame,freq:Int)={

    val tbName = "zhjg.app_comm_pri"
    dropPartition(spark,tbName,freq)
    df.createOrReplaceTempView("data")
    val sql = "SELECT a.index_id,a.index_name,a.value,a.unit,a.`date` data_time,a.area_name,a.frequence_id,a.year,a.month,a.day from data a WHERE a.frequence='日' AND a.index_type='价格' AND (a.area_name ='全国' or a.area_name = '中国')"
    val data: DataFrame = spark.sql(sql)
    data.coalesce(1).write.format("hive").mode(saveMode = "append")
      .partitionBy("frequence_id","year","month","day")
      .saveAsTable(tbName)
    data
  }

  private def dropPartition(spark: SparkSession,tbName:String,freq:Int)={
    var date=""
    if(freq==globalConfUtils.day){
      date=dateUtils.getYesterday
    }else if(freq==globalConfUtils.week){
      date=dateUtils.getLastWeekN(5)
    }else if(freq==globalConfUtils.mouth){
      date=dateUtils.getLastMonthEnd
    }

    val strings: Array[String] = date.split("-")
    val year: String = strings(0)
    val month: String = strings(1)
    val day: String = strings(2)
    val sql = "ALTER TABLE "+tbName+" DROP IF EXISTS PARTITION (frequence_id="+freq+",year="+year+",month="+month+",day="+day+")"
    spark.sql(sql)
  }
}
