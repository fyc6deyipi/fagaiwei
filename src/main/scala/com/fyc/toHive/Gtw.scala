package com.fyc.toHive

import java.util

import com.fyc.config.globalConfUtils
import com.fyc.utils.dateUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DateType, FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._

object Gtw {
    def data2hive(sc:SparkContext,spark:SparkSession,list:util.List[String])={
      val array: Array[String] = list.asScala.toArray
      val data: RDD[Row] = sc.parallelize(array,numSlices = 10000).map(_.split(",")).map(
        arr=>
          Row(arr(0),if("null".equals(arr(1))||"-".equals(arr(1))) Float.NaN else arr(1).toFloat,dateUtils.toSqlDate(arr(2)),arr(3),arr(4),arr(5)))
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
      val result: DataFrame = spark.sql("select gbm.index_id,d.value,d.date,gbm.frequence_id,d.year,d.month,d.day from data d left join zhjg.gtw_base_map gbm on d.index_id=gbm.source_index_id")


      result.coalesce(1).write.format("hive").mode(saveMode = "overwrite")
        .partitionBy("frequence_id","year","month","day")
        .saveAsTable("zhjg.gtw_value")
      result
    }
    def data2hive(sc:SparkContext,spark:SparkSession,list:util.List[String],date:String,frequence_id: Int)={
      val array: Array[String] = list.asScala.toArray
      val data: RDD[Row] = sc.parallelize(array,numSlices = 10000).map(_.split(",")).map(
        arr=>
          Row(arr(0),if("null".equals(arr(1))||"-".equals(arr(1))) Float.NaN else arr(1).toFloat,dateUtils.toSqlDate(arr(2)),arr(3),arr(4),arr(5)))
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
      val strings: Array[String] = date.split("-")
      val year: String = strings(0)
      val month: String = strings(1)
      val day: String = strings(2)
      val sql = "ALTER TABLE zhjg.gtw_value DROP IF EXISTS PARTITION (frequence_id="+frequence_id+",year="+year+",month="+month+",day="+day+")"
      spark.sql(sql)
      df.createOrReplaceTempView("data")
      val result: DataFrame = spark.sql("select gbm.index_id,d.value,d.date,gbm.frequence_id,d.year,d.month,d.day from data d left join zhjg.gtw_base_map gbm on d.index_id=gbm.source_index_id")
      result.show(10000)
      result.coalesce(1).write.format("hive").mode(saveMode = "append")
        .partitionBy("frequence_id","year","month","day")
        .saveAsTable("zhjg.gtw_value")
      result
    }
}
