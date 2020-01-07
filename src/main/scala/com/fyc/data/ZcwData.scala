package com.fyc.data

import org.apache.spark.SparkContext
import com.fyc.`trait`.GetData

class ZcwData extends GetData {

  def zcw(sc:SparkContext)={
    sc.textFile("hdfs://10.1.3.238:8020//user/zhjg/zcw_data").collect()


  }

}
object ZcwData extends ZcwData{}
