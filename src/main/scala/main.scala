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

    val zoneLookup = sparkContext.textFile("data/taxi_zone_lookup.csv", 10)
    // heuristikom se doslo do broja 10

    println(zoneLookup.count())

    val jedinstveneVoznje = zoneLookup.distinct()
    println(jedinstveneVoznje.count())
  }
}