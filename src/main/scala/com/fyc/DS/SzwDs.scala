package com.fyc.DS

import java.util

import com.fyc.app.{sc, spark}
import com.fyc.`trait`.DS
import com.fyc.data.SzwData
import com.fyc.toHive.Szw
import com.fyc.utils.dateUtils
import org.apache.spark.sql.DataFrame

class SzwDs extends DS{

  def getSzwByFreq(dataDate:String,frequence_id:Int):DataFrame={
    var frame: DataFrame=null
    if (frequence_id==28){
      val list: util.List[String] = SzwData.getByDay(dataDate)
      val frame1: DataFrame = Szw.data2hive(sc,spark,list,dataDate,frequence_id)
      frame = frame1
    }
    if(frequence_id==29){
      val list: util.List[String] = SzwData.getByWeek(dataDate)
      val frame2: DataFrame = Szw.data2hive(sc,spark,list,dataDate,frequence_id)
      frame = frame2
    }
    frame

  }

  def getSzwAll={
    val list: util.ArrayList[String] = SzwData.getAll()
    val s: String = dateUtils.getSzwBegin
    val arr: Array[String] = s.split("-")
    var date = ""
    for (elem <- arr) {
      date+=elem
    }
    Szw.data2hive(sc,spark,list,date)
  }
}
object SzwDs extends SzwDs
