package com.fyc.DS

import java.util

import com.fyc.app.{sc, spark}
import com.fyc.`trait`.DS
import com.fyc.data.GtwData
import com.fyc.toHive.Gtw
import org.apache.spark.sql.DataFrame

class GtwDs extends DS{
  def getGtwAll = {
    val strings: util.List[String] = GtwData.getAll(spark)
    val frame: DataFrame = Gtw.data2hive(sc,spark,strings)
    frame
  }
  def getGtwByFreq(date:String,frequence_id:Int) = {
    val strings: util.List[String] = GtwData.getByFreq(spark,frequence_id)

    Gtw.data2hive(sc,spark,strings,date,frequence_id)
  }

}
object GtwDs extends GtwDs
