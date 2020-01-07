package com.fyc.DS

import com.fyc.config.globalConfUtils
import com.fyc.utils.dateUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object DSUtil {
  def main(args: Array[String]): Unit = {



  }
  def init(spark: SparkSession)={
    GtwDs.getGtwAll
    SzwDs.getSzwAll
    LzshDs.getLzshAll
    TxdsDs.getTxdsAll

    val result: DataFrame = spark.sql("select * from zhjg.gtw_value " +
      "union all " +
      "select * from zhjg.szw_value " +
      "union all " +
      "select * from zhjg.lzsh_value" +
      "union all " +
      "select * from zhjg.txds_value")
    result.coalesce(1).write.format("hive").mode(saveMode = "overwrite")
      .partitionBy("frequence_id","year","month","day")
      .saveAsTable("zhjg.wp_base_index_val")
  }
//  def DSByDay(spark: SparkSession)={
//    val yesterday: String = dateUtils.getYesterday
//    val freq: Int = globalConfUtils.day
//    val a: DataFrame = GtwDs.getGtwByFreq(yesterday,freq)
//    a.createOrReplaceTempView("a")
//    val b: DataFrame = SzwDs.getSzwByFreq(yesterday, freq)
//    b.createOrReplaceTempView("b")
//    val c: DataFrame = LzshDs.getLzshByFreq(yesterday,freq)
//    c.createOrReplaceTempView("c")
//    val d: DataFrame = LzshDs.getLzshByFreq(yesterday,freq)
//    d.createOrReplaceTempView("d")
//    val result: DataFrame = spark.sql("select * from a " +
//      "union all " +
//      "select * from b " +
//      "union all " +
//      "select * from c" +
//      "union all " +
//      "select * from d")
//
//    val strings: Array[String] = yesterday.split("-")
//    val year: String = strings(0)
//    val month: String = strings(1)
//    val day: String = strings(2)
//    val sql = "ALTER TABLE zhjg.wp_base_index_val DROP IF EXISTS PARTITION (frequence_id="+freq+",year="+year+",month="+month+",day="+day+")"
//    spark.sql(sql)
//
//    result.coalesce(1).write.format("hive").mode(saveMode = "append")
//      .partitionBy("frequence_id","year","month","day")
//      .saveAsTable("zhjg.wp_base_index_val")
//
//    result
//  }
  def DSByFreq(spark: SparkSession,freq:Int)={
    var date = ""
    if(freq==globalConfUtils.day){
      date = dateUtils.getYesterday
    }else if (freq==globalConfUtils.week){
      date = dateUtils.getLastWeekN(5)
    }else if(freq == globalConfUtils.mouth){
      date = dateUtils.getLastMonthEnd
    }
  val a: DataFrame = GtwDs.getGtwByFreq(date,freq)
  a.createOrReplaceTempView("a")
  val b: DataFrame = LzshDs.getLzshByFreq(date,freq)
  b.createOrReplaceTempView("b")
  var sql ="select * from a " +
    "union all " +
    "select * from b "
  if (freq != globalConfUtils.mouth){
      val c: DataFrame = SzwDs.getSzwByFreq(date, freq)
      c.createOrReplaceTempView("c")
      val d: DataFrame = TxdsDs.getTxdsByFreq(date,freq)
      d.createOrReplaceTempView("d")
      sql+=
        "union all " +
        "select * from c "+
        "union all " +
        "select * from d"
    }
    val result: DataFrame = spark.sql(sql)

    val strings: Array[String] = date.split("-")
    val year: String = strings(0)
    val month: String = strings(1)
    val day: String = strings(2)
    sql = "ALTER TABLE zhjg.wp_base_index_val DROP IF EXISTS PARTITION (frequence_id="+freq+",year="+year+",month="+month+",day="+day+")"
    spark.sql(sql)

    result.coalesce(1).write.format("hive").mode(saveMode = "append")
      .partitionBy("frequence_id","year","month","day")
      .saveAsTable("zhjg.wp_base_index_val")

    result
  }
}
