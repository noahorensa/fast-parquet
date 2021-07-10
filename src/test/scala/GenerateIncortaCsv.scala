import java.sql.Timestamp
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.MutableList

object GenerateIncortaCsv {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("parquet-resource-generator")

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    var dataList = new MutableList[(String, Double, Int, Long, String, String, String)]
    for (i <- 0 to 7999999) {
      val d = new Date(i*24l*60l*60l*1000l)
      val ds = d.getYear + "-" + d.getMonth + "-" + d.getDay
      dataList += ((ds, i, i, i, "test string_" + i, "test_text_" + i, new Timestamp(i).toString()))
      if (i % 100000 == 0) {
        println(i)
      }
    }
    println(dataList.length)
    val data8RDD = sc.parallelize(dataList, 2)

    val df8 = data8RDD.toDF()
    df8.write.format("csv").mode(SaveMode.Overwrite).save("resources/test_datatypes_csv_8mx7x2")

  }
}
