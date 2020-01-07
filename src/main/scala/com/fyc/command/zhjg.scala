package com.fyc.command

import com.fyc.DS.DSUtil
import com.fyc.toDB.{ Freq2DB, hive2db}
import com.fyc.concordance.{app, extract}
import org.apache.spark.sql.{DataFrame, SparkSession}

object zhjg {
  def main(args: Array[String]): Unit = {
//    scheduleByDay()
  }

  //over
  def init(spark:SparkSession)={
    DSUtil.init(spark)
    app.init(spark)
    hive2db.init(spark)

  }
  //over
//  def scheduleByDay(spark:SparkSession)={
//    val df: DataFrame = DSUtil.DSByDay(spark)
//    val tuple: (DataFrame, DataFrame) = extract.byDay(df,spark)
//    Freq2DB.run(spark,tuple,28)
//  }
  def scheduleByFreq(spark:SparkSession,freq: Int)={

    val df: DataFrame = DSUtil.DSByFreq(spark,freq)
    val tuple: (DataFrame,DataFrame) = extract.byFreq(df, spark, freq)
    Freq2DB.run(spark,tuple,freq)
  }


}
