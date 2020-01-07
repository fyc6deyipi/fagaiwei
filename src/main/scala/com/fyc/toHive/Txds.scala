package com.fyc.toHive

import java.util

import com.fyc.utils.dateUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DateType, FloatType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.collection.JavaConverters._


object Txds {
  def data2hive(sc: SparkContext, spark: SparkSession, list: util.List[String]) = {
    val array: Array[String] = list.asScala.toArray
    val data: RDD[Row] = sc.parallelize(array,numSlices = 10000).map(_.split(",")).map(
      arr=>
        Row(arr(0),arr(1).toFloat,dateUtils.toSqlDate(arr(2)),arr(3),arr(4),arr(5)))
    val structType: StructType = StructType(List(
      StructField("index_id", StringType, true),
      StructField("value", FloatType, false),
      StructField("date", DateType, true),
      StructField("year", StringType, true),
      StructField("month", StringType, true),
      StructField("day", StringType, true)
    ))
    val df: DataFrame = spark.createDataFrame(data,structType)
    df.createOrReplaceTempView("data")

    spark.sql("use zhjg")
    val result: DataFrame = spark.sql("select sbm.index_id,d.value,d.date,sbm.frequence_id,d.year,d.month,d.day from data d left join txds_base_map sbm on d.index_id=sbm.source_index_id").na.drop()
    result.show(10000)

    result
      .coalesce(1).write.format("hive").mode(saveMode = "append")
      .saveAsTable("zhjg.txds_value_history")
    spark.sql("select * from zhjg.txds_value_history")
      .coalesce(1).write.format("hive").mode(saveMode = "overwrite")
      .partitionBy("frequence_id","year","month","day")
      .saveAsTable("zhjg.txds_value")


  }
  def data2hive(sc: SparkContext, spark: SparkSession, list: util.List[String],date:String,frequence_id:Int) = {
    val array: Array[String] = list.asScala.toArray
    val data: RDD[Row] = sc.parallelize(array,numSlices = 10000).map(_.split(",")).map(
      arr=>
        Row(arr(0),arr(1).toFloat,dateUtils.toSqlDate(arr(2)),arr(3),arr(4),arr(5)))
    val structType: StructType = StructType(List(
      StructField("index_id", StringType, true),
      StructField("value", FloatType, false),
      StructField("date", DateType, true),
      StructField("year", StringType, true),
      StructField("month", StringType, true),
      StructField("day", StringType, true)
    ))
    val df: DataFrame = spark.createDataFrame(data,structType)
    df.createOrReplaceTempView("data")

    spark.sql("use zhjg")
    val result: DataFrame = spark.sql("select sbm.index_id,d.value,d.date,sbm.frequence_id,d.year,d.month,d.day from data d left join txds_base_map sbm on d.index_id=sbm.source_index_id")
      .na.drop()
    result.show(1000)

    val strings: Array[String] = date.split("-")
    var year: String = strings(0)
    var month: String = strings(1)
    var day: String = strings(2)
    val sql = "ALTER TABLE zhjg.txds_value DROP IF EXISTS PARTITION (frequence_id="+frequence_id+",year="+year+",month="+month+",day="+day+")"
    spark.sql(sql)
    if (dateUtils.isWeekN(date,7)){
      val Friday: Array[String] = dateUtils.date2WeekN(date, 5).split("-")
      year = Friday(0)
      month = Friday(1)
      day = Friday(2)
      val sql = "ALTER TABLE zhjg.txds_value DROP IF EXISTS PARTITION (frequence_id="+frequence_id+",year="+year+",month="+month+",day="+day+")"
      spark.sql(sql)
    }
    result.show(1000)
    result
      .coalesce(1).write.format("hive").mode(saveMode = "append")
      .partitionBy("frequence_id","year","month","day")
      .saveAsTable("zhjg.txds_value")
    result
      .coalesce(1).write.format("hive").mode(saveMode = "overwrite")
      .saveAsTable("zhjg.txds_value_history")
    result

  }
}