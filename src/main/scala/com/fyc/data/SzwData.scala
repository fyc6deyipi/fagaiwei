package com.fyc.data

import java.util
import java.util.{ArrayList, Collections, Date, HashMap, Map}

import com.fyc.getJson.SzwJson
import com.fyc.`trait`.GetData
import com.fyc.parseJson.ParseSzw
import com.fyc.config.globalConfUtils
import com.fyc.utils.{dateUtils}
import org.apache.commons.codec.digest.DigestUtils



class SzwData extends GetData{

  private val count = 179

  def getSign(date: String, datatype: String)={
    val list = new util.ArrayList[String]
    val map = new util.HashMap[String, String]
    //获取时间戳
    //        String timestamp = "1573198110006";
    val timestamp: String = new Date().getTime.toString
    val key = globalConfUtils.szwKey
    map.put("date", date)
    map.put("datatype", datatype)
    map.put("timestamp", timestamp)

    list.add("datatype")
    list.add("date")
    list.add("timestamp")

    //参数值字典排序
    Collections.sort(list)
    //拼接前三个参数
    val str: String = list.get(0) + "=" + map.get(list.get(0)) + "&" + list.get(1) + "=" + map.get(list.get(1)) + "&" + list.get(2) + "=" + map.get(list.get(2))
    //前三个参数拼接sign，sign MD5加密并字符转大写
    val sign: String = str + "&sign=" + DigestUtils.md5Hex(str + "&key=" + key).toUpperCase
    sign
  }

  def getByDay(date:String)={
    val param: String = getSign(date,"1")
    val json:String = SzwJson.get(param)
    ParseSzw.runByDay(json)
  }

  def getByWeek(date:String)={
    val param: String = getSign(date,"2")
    val json:String = SzwJson.get(param)
    ParseSzw.runByWeek(json,date)
  }

  private def getAllOfDay()={
    val begin = dateUtils.getSzwBegin;
    val list = new util.ArrayList[String]()
    for( i <- 0 to count-1){
      val date: String = dateUtils.dateAddFrequence(begin,i)
      println(date)
      val buffer: util.List[String] = getByDay(date)
      list.addAll(buffer)
    }
    list
  }

  private def getAllOfWeek()={

    val begin = dateUtils.date2WeekN(dateUtils.getSzwBegin,5);
    val list = new util.ArrayList[String]()
    for( i <- 0 to (count-1)/7){
      val date: String = dateUtils.dateAddFrequence(begin,i*7)
      println(date)
      val buffer: util.List[String] = getByWeek(date)
      list.addAll(buffer)
    }
    list
  }

  def getAll()={
    val all = new util.ArrayList[String]()
    val listDay: util.ArrayList[String] = getAllOfDay()
    val listWeek: util.ArrayList[String] = getAllOfWeek()
    all.addAll(listWeek)
    all.addAll(listDay)
    all
  }

}
object SzwData extends SzwData
