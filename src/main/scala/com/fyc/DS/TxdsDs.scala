package com.fyc.DS

import java.util

import com.fyc.app.{sc, spark}
import com.fyc.`trait`.DS
import com.fyc.data.TxdsData
import com.fyc.toHive.Txds
import com.fyc.config.globalConfUtils
import com.fyc.utils.dateUtils
import org.apache.spark.sql.DataFrame

class TxdsDs extends DS{

  def getTxdsAll()={
    val strings: util.List[String] = TxdsData.get()
    Txds.data2hive(sc,spark,strings)
  }
  def getTxdsByFreq(date:String,frequence_id:Int): DataFrame={
    if (frequence_id==globalConfUtils.week){
      val strings: Array[String] = dateUtils.date2LastWeekN(date,5).split("-")
      val year: String = strings(0)
      val month: String = strings(1)
      val day: String = strings(2)
      val frame: DataFrame = spark.sql("select * from zhjg.txds_value where frequence_id=" + frequence_id + " and year=" + year + " and month=" + month + " and day=" + day)
      return frame
    }

    val data: util.List[String] = TxdsData.get()
    Txds.data2hive(sc,spark,data,date,frequence_id)
  }
}
object TxdsDs extends TxdsDs