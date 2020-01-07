package com.fyc.toHive


import com.fyc.config.globalConfUtils
import com.fyc.data.LzshData
import com.fyc.utils.dateUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DateType, FloatType, StringType, StructField, StructType}

object Lzsh {
  def data2hive(spark: SparkSession)={
    val df: DataFrame = LzshData.getAll(spark)
    val structType: StructType = StructType(List(
      StructField("index_id", StringType, true),
      StructField("value", FloatType, false),
      StructField("date", DateType, true),
      StructField("year", StringType, true),
      StructField("month", StringType, true),
      StructField("day", StringType, true)
    ))

    val rdd: RDD[Row] = df.rdd.map(
      row =>
        Row(row.getAs[String](0),row.getAs[String](1).toFloat,dateUtils.toSqlDate(row.getAs[String](2)),row.getAs[String](2).split("-")(0),row.getAs[String](2).split("-")(1),row.getAs[String](2).split("-")(2))
    )
    val data: DataFrame = spark.createDataFrame(rdd,structType)
    data.createOrReplaceTempView("data")
    spark.sql("use zhjg")
    data.show()
    val result: DataFrame = spark.sql("select l.index_id,d.value,d.date,l.frequence_id,d.year,d.month,d.day from data d left join lzsh_base_map l on d.index_id = l.source_index_id")
    result.show(9999)
    result.coalesce(1).write.format("hive").mode(saveMode = "overwrite")
      .partitionBy("frequence_id","year","month","day")
      .saveAsTable("zhjg.lzsh_value")
    result
  }
  def data2hive(spark: SparkSession,date:String,frequence_id:Int)={

    val df: DataFrame = LzshData.getByFreq(spark,frequence_id)
    df.show(1000)
    df.printSchema()
    val structType: StructType = StructType(List(
      StructField("index_id", StringType, true),
      StructField("value", FloatType, false),
      StructField("date", DateType, true),
      StructField("year", StringType, true),
      StructField("month", StringType, true),
      StructField("day", StringType, true)
    ))
    val rdd: RDD[Row] =
        df.rdd.map(
          row =>
            Row(row.getAs[String](0),row.getAs[String](1).toFloat,dateUtils.toSqlDate(row.getAs[String](2)),row.getAs[String](2).split("-")(0),row.getAs[String](2).split("-")(1),row.getAs[String](2).split("-")(2)))
    val data: DataFrame = spark.createDataFrame(rdd,structType)
    val strings: Array[String] = date.split("-")
    val year: String = strings(0)
    val month: String = strings(1)
    val day: String = strings(2)
    val sql = "ALTER TABLE zhjg.lzsh_value DROP IF EXISTS PARTITION (frequence_id="+frequence_id+",year="+year+",month="+month+",day="+day+")"
    spark.sql(sql)
    data.createOrReplaceTempView("data")

    spark.sql("use zhjg")
    val result: DataFrame = spark.sql("select l.index_id,d.value,d.date,l.frequence_id,d.year,d.month,d.day from data d left join (select * from lzsh_base_map where frequence_id = "+frequence_id+") l on d.index_id = l.source_index_id")
    result.na.drop().coalesce(1).write.format("hive").mode(saveMode = "append")
      .partitionBy("frequence_id","year","month","day")
      .saveAsTable("zhjg.lzsh_value")
    result
  }

}
