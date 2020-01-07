package com.fyc.DS

import com.fyc.app.spark
import com.fyc.`trait`.DS
import com.fyc.toHive.Lzsh
import org.apache.spark.sql.DataFrame

class LzshDs extends DS{

  def getLzshAll={
    val frame: DataFrame = Lzsh.data2hive(spark)
    frame
  }
  def getLzshByFreq(dataDate:String,frequence_id:Int)={
    Lzsh.data2hive(spark,dataDate,frequence_id)
  }
}
object LzshDs extends LzshDs