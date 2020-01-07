package com.fyc.toHive

import java.util

import com.fyc.utils.dateUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DateType, FloatType, StringType, StructField, StructType}
import scala.collection.JavaConverters._

object Szw {
  def data2hive(sc:SparkContext,spark:SparkSession,list:util.List[String],date:String,frequence_id: Int)={
    val array: Array[String] = list.asScala.toArray
    val data: RDD[Row] = sc.parallelize(array,numSlices = 10000).map(_.split(",")).map(
      arr=>
        Row(arr(0),if("".equals(arr(1))) Float.NaN else arr(1).toFloat,dateUtils.toSqlDate(arr(2)),arr(3),arr(4),arr(5)))
    val structType: StructType = StructType(List(
      StructField("index_id", StringType, true),
      StructField("value", FloatType, false),
      StructField("date", DateType, true),
      StructField("year", StringType, true),
      StructField("month", StringType, true),
      StructField("day", StringType, true)
    ))
    val df: DataFrame = spark.createDataFrame(data,structType)
    df.show(1000)
    df.createOrReplaceTempView("data")
    spark.sql("use zhjg")
    val result: DataFrame = spark.sql("select sbm.index_id,d.value,d.date,sbm.frequence_id,d.year,d.month,d.day from data d left join szw_base_map sbm on d.index_id=sbm.szw_index_id").na.drop()
    result.show(1000)

    val strings: Array[String] = date.split("-")
    val year: String = strings(0)
    val month: String = strings(1)
    val day: String = strings(2)
    val sql = "ALTER TABLE zhjg.lzsh_value DROP IF EXISTS PARTITION (frequence_id="+frequence_id+",year="+year+",month="+month+",day="+day+")"
    spark.sql(sql)

    result.coalesce(1).write.format("hive").mode(saveMode = "append")
      .partitionBy("frequence_id","year","month","day")
      .saveAsTable("zhjg.szw_value")
    result
  }
  def data2hive(sc:SparkContext,spark:SparkSession,list:util.List[String],date:String)={
    val array: Array[String] = list.asScala.toArray
    val data: RDD[Row] = sc.parallelize(array,numSlices = 10000).map(_.split(",")).map(
      arr=>
        Row(arr(0),if("".equals(arr(1))) Float.NaN else arr(1).toFloat,dateUtils.toSqlDate(arr(2)),arr(3),arr(4),arr(5)))
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
    val result: DataFrame = spark.sql("select sbm.index_id,d.value,d.date,sbm.frequence_id,d.year,d.month,d.day from data d left join szw_base_map sbm on d.index_id=sbm.szw_index_id").na.drop()
    result.show()
    result.createOrReplaceTempView("a")
    spark.sql("select * from a from where frequence_id is null").show()

    result
      .coalesce(1).write.format("hive").mode(saveMode = "overwrite")
      .partitionBy("frequence_id","year","month","day")
      .saveAsTable("zhjg.szw_value")

    spark.sql("select * from szw_value")
      .coalesce(1).write.format("hive").mode(saveMode = "overwrite")
      .saveAsTable("zhjg.szw_value_backup_"+date)
    result
  }

}
