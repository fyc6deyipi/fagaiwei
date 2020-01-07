package com.fyc.toHive

import java.util

import com.fyc.pojo.heatTrend
import com.fyc.utils.dateUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DateType, FloatType, IntegerType, LongType, StringType, StructField, StructType}

object Zcw {
  def data2hive(sc: SparkContext, spark: SparkSession, lines:Array[String]) = {


    val data: RDD[Row] = sc.parallelize(lines).map(_.split(",")).map(
      arr=>
        Row(arr(0),if ("".equals(arr(1))) Float.NaN else arr(1).toFloat,dateUtils.toSqlDate(arr(2)),arr(2).split("-")(0),arr(2).split("-")(1),arr(2).split("-")(2)))
    val structType: StructType = StructType(List(
      StructField("source_index_id",StringType, true),
      StructField("value", FloatType, false),
      StructField("date", DateType, true),
      StructField("year", StringType, true),
      StructField("month", StringType, true),
      StructField("day", StringType, true)
    ))
    val df: DataFrame = spark.createDataFrame(data,structType)
    df.createOrReplaceTempView("data")
    df.show()
    df.printSchema()

    spark.sql("use zhjg")
    val result: DataFrame = spark.sql("select b.index_id,a.value,a.date,b.frequence_id,a.year,a.month,a.day from  data a left join zhjg.zcw_base_map b on a.source_index_id =b.source_index_id")
    result.printSchema()
    result.show()
    result
      .coalesce(1).write.format("hive").mode(saveMode = "overwrite")
      .partitionBy("frequence_id","year","month","day")
      .saveAsTable("zhjg.zcw_value")


  }

}
