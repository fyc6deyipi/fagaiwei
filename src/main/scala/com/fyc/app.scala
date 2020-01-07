package com.fyc

import com.fyc.DS.TxdsDs
import com.fyc.command.zhjg
import com.fyc.config.globalConfUtils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}


object app {
  val conf: SparkConf = new SparkConf().setAppName("day")
    .setMaster("local[8]")
  val spark = SparkSession
    .builder()
    .config(conf)
    .enableHiveSupport()
    .config("spark.sql.adaptive.enabled",true)
    .config("spark.sql.adaptive.shuffle.targetPostShuffleInputSize",134217728)
    .config("hive.exec.dynamic.partition", true) // 支持 Hive 动态分区
    .config("hive.exec.dynamic.partition.mode", "nonstrict") // 非严格模式
    .getOrCreate()
  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("WARN")
  def main(args: Array[String]): Unit = {
    zhjg.init(spark)
    zhjg.scheduleByFreq(spark,globalConfUtils.week)
//    TxdsDs.getTxdsByFreq(dateUtils.getToday)
  }

}
