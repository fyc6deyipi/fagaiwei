package com.fyc.config

import com.typesafe.config.{Config, ConfigFactory}

class globalConfUtils {

  val config: Config = ConfigFactory.load()

  //钢铁网
  def gtwUrl: String = config.getString("gtw_url")
  def token: String = config.getString("token")
  def startTime: String = config.getString("starttime")
  def endTime: String = config.getString("endtime")
  def order: String = config.getString("order")
  def indexUrl:String = config.getString("index_url")

  //搜猪网
  def szwUrl: String = config.getString("szw_url")
  def szwKey: String = config.getString("szw_key")

  //腾讯电商
  def txdsUrl:String = config.getString("txds_url")
  def secretKey:String = config.getString("secretKey")
  def requestJson:String = config.getString("requestJson")

  //腾讯舆情
  def appid: String = config.getString("apppid")
  def password: String = config.getString("password")
  def txyqUrl: String = config.getString("txyq_url")

  //隆众石化
  def lzsh_driver:String = config.getString("lzsh.driver")
  def lzsh_url:String = config.getString("lzsh.url")
  def lzsh_username:String = config.getString("lzsh.username")
  def lzsh_password:String = config.getString("lzsh.password")
  //隆众石化
  def mysql_driver:String = config.getString("mysql.driver")
  def mysql_url:String = config.getString("mysql.url")
  def mysql_username:String = config.getString("mysql.username")
  def mysql_password:String = config.getString("mysql.password")

  //频度
  def day:Int=config.getInt("day")
  def week:Int=config.getInt("week")
  def xun:Int=config.getInt("xun")
  def mouth:Int=config.getInt("month")
  def quarter:Int=config.getInt("quarter")
  def year:Int=config.getInt("year")

}
object globalConfUtils extends globalConfUtils
