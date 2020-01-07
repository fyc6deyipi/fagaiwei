package com.fyc.toDB

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object hive2db {

  def init(spark: SparkSession)={
    Init.init(spark)
  }

}


