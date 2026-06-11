import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {

    println(System.getProperty("java.version"))
    println(System.getProperty("java.home"))

    val sparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

    val spark = SparkSession.builder().
      appName("Taxi Zone Lookup")
      .master("local[*]")
      .config(sparkConf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val sparkContext = spark.sparkContext

    query1(spark, sparkContext)
  }

  private def query1(sparkSession: SparkSession, sparkContext: SparkContext): Unit = {

    val tripdataDF = sparkSession.read.parquet("data/yellow_tripdata_2024-01.parquet")
    val tripdataRDD = tripdataDF.rdd

    val result = tripdataRDD
      .map(row => (
        (row.getAs[Int]("PULocationID"), row.getAs[Int]("DOLocationID")),
        (row.getAs[Double]("total_amount"), 1)
      ))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .mapValues { case (total, count) => total / count }
      .sortBy(_._2, ascending = false)

    result.take(10).foreach(println)
  }
}