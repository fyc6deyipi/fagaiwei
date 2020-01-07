package com.fyc.data

import com.fyc.`trait`.GetData
import com.fyc.config.globalConfUtils
import com.fyc.utils.dateUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

class LzshData extends GetData{
  def getAll(spark: SparkSession)={

    val sql: String =
        "(SELECT INDEX_CODE,DATA_VALUE,DATA_DATE FROM MYSTEEL_DATA WHERE DATA_DATE <='"+dateUtils.getYesterday+"')as t1"

    spark
      .read
      .format("jdbc")
      .option("url", globalConfUtils.lzsh_url)
      .option("driver", globalConfUtils.lzsh_driver)
      .option("user", globalConfUtils.lzsh_username)
      .option("password", globalConfUtils.lzsh_password)
      //        .option("dbtable", tableName.toUpperCase)
      .option("dbtable", sql)
      .load()
  }
  def getByFreq(spark: SparkSession,frequence_id:Int)={
    var sql = ""
    if (frequence_id == globalConfUtils.day){
      sql= "(SELECT INDEX_CODE,DATA_VALUE,DATA_DATE FROM MYSTEEL_DATA where DATA_DATE ='"+dateUtils.getYesterday+"')as t1"
    }else if(frequence_id == globalConfUtils.week){
      sql=
        "(SELECT a.* FROM " +
        "(SELECT INDEX_CODE,DATA_VALUE,DATA_DATE FROM MYSTEEL_DATA WHERE DATA_DATE BETWEEN '"+dateUtils.getLastWeekN(1)+"' AND '"+dateUtils.getLastWeekN(7)+"') a " +
        "inner JOIN " +
        "(SELECT * FROM MYSTEEL_INDEX  WHERE frequency = '周度') b " +
        "ON a.index_code = b.index_code" +
        ")as t1"
    }else if(frequence_id == globalConfUtils.mouth){
      sql=
          "(SELECT a.* FROM " +
          "(SELECT INDEX_CODE,DATA_VALUE,DATA_DATE FROM MYSTEEL_DATA WHERE DATA_DATE BETWEEN '"+dateUtils.getLastMonthFirst()+"' AND '"+dateUtils.getLastMonthEnd()+"') a " +
          "inner JOIN " +
          "(SELECT * FROM MYSTEEL_INDEX  WHERE frequency = '月度') b " +
          "ON a.index_code = b.index_code" +
          ")as t1"
    }

    spark
      .read
      .format("jdbc")
      .option("url", globalConfUtils.lzsh_url)
      .option("driver", globalConfUtils.lzsh_driver)
      .option("user", globalConfUtils.lzsh_username)
      .option("password", globalConfUtils.lzsh_password)
      //        .option("dbtable", tableName.toUpperCase)
      .option("dbtable", sql)
      .load()

  }
  def getByDay(spark: SparkSession,date:String,frequence_id:Int)={
    var sql = ""
    if (frequence_id == globalConfUtils.day){
      sql= "SELECT INDEX_CODE,DATA_VALUE,DATA_DATE FROM MYSTEEL_DATA where DATA_DATE ='"+dateUtils.getYesterday+"')as t1"
    }else if(frequence_id == globalConfUtils.week){
      sql= "SELECT INDEX_CODE,DATA_VALUE,DATA_DATE FROM MYSTEEL_DATA where DATA_DATE between'"+dateUtils.getLastWeekN(1)+"' and '"+dateUtils.getLastWeekN(7)+"')as t1"
    }else if(frequence_id == globalConfUtils.mouth){
      sql= "SELECT INDEX_CODE,DATA_VALUE,DATA_DATE FROM MYSTEEL_DATA where DATA_DATE between'"+dateUtils.getLastMonthFirst()+"' and '"+dateUtils.getLastMonthEnd()+"')as t1"
    }

    spark
      .read
      .format("jdbc")
      .option("url", globalConfUtils.lzsh_url)
      .option("driver", globalConfUtils.lzsh_driver)
      .option("user", globalConfUtils.lzsh_username)
      .option("password", globalConfUtils.lzsh_password)
      //        .option("dbtable", tableName.toUpperCase)
      .option("dbtable", sql)
      .load()
  }
}
object LzshData extends LzshData
