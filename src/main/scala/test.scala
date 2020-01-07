import com.fyc.data.LzshData
import com.fyc.getJson.TxdsJson
import com.fyc.parseJson.ParseTxds
import com.fyc.utils.dateUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DateType, FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object test {
  val conf: SparkConf = new SparkConf().setAppName("app").setMaster("local[8]")

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

//    val df: DataFrame = LzshData.getByFreq(spark,29)
//    df.createOrReplaceTempView("data")
//    df.show(100)
//    val result: DataFrame = spark.sql("select *,row_number() over(partition by index_code order by data_date desc) num from data ")
//    result.show()
    val df: DataFrame = LzshData.getAll(spark)
    df.createOrReplaceTempView("data")
    df.show(100)
    val result: DataFrame = spark.sql("select *,row_number() over(partition by index_code,data_date order by update_time desc) num from data ")
    result.show(1000)
    result.coalesce(1).write.csv("C:\\Users\\DFJX\\Desktop\\发改委智慧价格\\隆众石化——终端\\all")



  }

}
