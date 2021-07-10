import java.sql.Date

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.MutableList

object GenerateTestResources {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("parquet-resource-generator")

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    sc.parallelize(List(
      (101, "John",     "Smith",                new Date(65, 1, 5),   "CEO",                    100000.5,          1),
      (102, "John",     "Not Smith",            new Date(64, 11, 5),  "Manager of Stuff",       100.0,             1),
      (103, "John",     "Definitely Not Smith", new Date(30, 2, 9),   "Not CEO",                1.11,              1),
      (104, "Mann",     "The Man",              new Date(66, 9, 21),  "Some Guy",               999.5,             1),
      (105, "Janet",    "Smith",                new Date(64, 10, 20), "CTO",                    999999.9,          1),
      (106, "Kirk",     "Taylor",               new Date(55, 11, 6),  "Chief Marketing",        10000.5,           2),
      (107, "Lana",     "Lola",                 new Date(42, 9, 9),   "COO",                    89999.0,           1),
      (108, "Sara",     "Look",                 new Date(59, 8, 7),   "Marketing Specialist",   1000.5,            2),
      (109, "Stan",     "The Man",              new Date(22, 12, 28), "Awesome man",            999999999.999,     3),
      (110, "Bob",      "Blart",                new Date(65, 7, 13),  "HR Manager",             10.5,              4)
    ), 1).toDF("id" ,"fname" , "lname", "birthday", "position", "salary", "department")
      .write.format("parquet").option("compression", "none").mode(SaveMode.Overwrite).save("resources/employees")

    sc.parallelize(List(
      (1, "Top Management"),
      (2, "Marketing"),
      (3, "Awesomeness"),
      (4, "HR"),
    ), 1).toDF("id", "name")
      .write.format("parquet").option("compression", "none").mode(SaveMode.Overwrite).save("resources/departments")

    var dataList = new MutableList[Tuple16[Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long, Long]]
    for (i <- 0 to 1999999) {
      dataList += ((i, i+1, i+2, i+3, i+4, i+5, i+6, i+7, i+8, i+9, i+10, i+11, i+12, i+13, i+14, i+15))
      if (i % 100000 == 0) {
        println(i)
      }
    }
    println(dataList.length)
    val data2RDD = sc.parallelize(dataList, 1)

    val df2 = data2RDD.toDF()
    df2.write.format("parquet").option("compression", "none").mode(SaveMode.Overwrite).save("resources/test_2mx16x1")
    df2.write.format("parquet").option("compression", "snappy").mode(SaveMode.Overwrite).save("resources/test_2mx16x1_snappy")

    for (i <- 2000000 to 7999999) {
      dataList += ((i, i+1, i+2, i+3, i+4, i+5, i+6, i+7, i+8, i+9, i+10, i+11, i+12, i+13, i+14, i+15))
      if (i % 100000 == 0) {
        println(i)
      }
    }
    println(dataList.length)
    val data8RDD = sc.parallelize(dataList, 8)

    val df8 = data8RDD.toDF()
    df8.write.format("parquet").option("compression", "none").mode(SaveMode.Overwrite).save("resources/test_8mx16x8")
    df8.write.format("parquet").option("compression", "snappy").mode(SaveMode.Overwrite).save("resources/test_8mx16x8_snappy")

    val df16 = df8.union(df8)
    df16.write.format("parquet").option("compression", "none").mode(SaveMode.Overwrite).save("resources/test_16mx16x16")
    df16.write.format("parquet").option("compression", "snappy").mode(SaveMode.Overwrite).save("resources/test_16mx16x16_snappy")

    val df32 = df16.union(df16)
    df32.write.format("parquet").option("compression", "none").mode(SaveMode.Overwrite).save("resources/test_32mx16x32")
    df32.write.format("parquet").option("compression", "snappy").mode(SaveMode.Overwrite).save("resources/test_32mx16x32_snappy")

    val df64 = df32.union(df32)
    df64.write.format("parquet").option("compression", "none").mode(SaveMode.Overwrite).save("resources/test_64mx16x64")
    df64.write.format("parquet").option("compression", "snappy").mode(SaveMode.Overwrite).save("resources/test_64mx16x64_snappy")
  }
}
