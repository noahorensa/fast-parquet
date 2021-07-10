import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}

object ParquetReadTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName("parquet-read-test")

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()

    val df = spark.read.parquet("resources/test_2mx16x1_snappy")

    val t0 = System.nanoTime

    df.write.format("csv").mode(SaveMode.Overwrite).save("resources/spark_csv")

    val t1 = System.nanoTime
    System.out.println("finished load in " + (t1 - t0) / 1e9 + "s")
  }
}
