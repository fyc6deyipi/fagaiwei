package com.fyc.data

import java.util

import com.fyc.getJson.GtwJson
import com.fyc.`trait`.GetData
import com.fyc.parseJson.ParseGtw
import com.fyc.config.globalConfUtils
import com.fyc.utils.dateUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.JavaConverters._

class GtwData extends GetData{
  private def getIndex(spark: SparkSession,frequence:Int):Array[String]={

    val SQL= "select a.source_index_id from zhjg.gtw_base_map a where a.frequence_id = '"+frequence+"'"
    val indexId: DataFrame = spark.sql(SQL)
    val indexsRdd: RDD[Row] = indexId.rdd
    val arr: Array[String] = indexsRdd.collect().map(_ (0).toString)
    arr
  }
  private def getIndex(spark: SparkSession):Array[String]={

    val SQL= "select a.source_index_id from zhjg.gtw_base_map a "
    val indexId: DataFrame = spark.sql(SQL)
    val indexsRdd: RDD[Row] = indexId.rdd
    val arr: Array[String] = indexsRdd.collect().map(_ (0).toString)
    arr
  }

  def getByFreq(spark: SparkSession,frequence_id:Int):util.List[String]={
    val indexs: Array[String] = getIndex(spark,frequence_id)
    var startTime: String = ""
    var endTime: String = ""
    if(frequence_id==globalConfUtils.day){
      startTime = dateUtils.dateToStamp(dateUtils.getYesterday)
    }else if (frequence_id==globalConfUtils.week){
      startTime = dateUtils.dateToStamp(dateUtils.getLastWeekN(1))
      endTime = dateUtils.dateToStamp(dateUtils.getLastWeekN(7))
    }else if(frequence_id == globalConfUtils.mouth){
      startTime = dateUtils.dateToStamp(dateUtils.getLastMonthFirst)
      endTime = dateUtils.dateToStamp(dateUtils.getLastMonthEnd)
    }

    val size: Int = indexs.length/150
    if (0 == size){
      return ParseGtw.run(GtwJson.get(indexs,startTime,endTime),frequence_id)
    }else{
      val data: util.ArrayList[String] = new util.ArrayList[String]()
      val arr: mutable.Buffer[String] = new util.ArrayList[String]().asScala

      for (i <- 0 to size){
        if(i == size){
          for (j <- i*150 to indexs.length-1 ){
            arr+=indexs(j)
          }
          data.addAll(ParseGtw.run(GtwJson.get(arr.toArray,startTime,endTime),frequence_id))
        }else{
          for (j <- i*150 to (i+1)*150-1 ){
            arr+=indexs(j)
          }
          data.addAll(ParseGtw.run(GtwJson.get(arr.toArray,startTime,endTime),frequence_id))
          arr.clear()
        }
      }
      println(data.size())
      return data

    }


  }

  def getAll(spark: SparkSession):util.List[String]={

    val data: util.List[String] = getAllByFreq(spark, globalConfUtils.day)
    data.addAll(getAllByFreq(spark,globalConfUtils.week))
    data.addAll(getAllByFreq(spark,globalConfUtils.mouth))
    data

  }

  private def getAllByFreq(spark: SparkSession,frequence_id:Int)={
    val indexs: Array[String] = getIndex(spark,frequence_id)
    var result: util.List[String] = new util.ArrayList[String]();

    val size: Int = indexs.length/150
    if (0 == size){
      result = ParseGtw.run(GtwJson.get(indexs), frequence_id)
    }else{
      val data: util.ArrayList[String] = new util.ArrayList[String]()
      val arr: mutable.Buffer[String] = new util.ArrayList[String]().asScala

      for (i <- 0 to size){
        if(i == size){
          for (j <- i*150 to indexs.length-1 ){
            arr+=indexs(j)
          }
          data.addAll(ParseGtw.run(GtwJson.get(arr.toArray),frequence_id))
        }else{
          for (j <- i*150 to (i+1)*150-1 ){
            arr+=indexs(j)
          }
          data.addAll(ParseGtw.run(GtwJson.get(arr.toArray),frequence_id))
          arr.clear()
        }
      }
      println(data.size())
      result = data
    }
    result
  }
}
object GtwData extends GtwData
