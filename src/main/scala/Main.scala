import com.sun.org.apache.xalan.internal.lib.ExsltDatetime.year
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{lit, to_date}
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

    println("***************************")

    query2(spark, sparkContext)

    println("***************************")

    query3(spark, sparkContext)

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

  private def query2(sparkSession: SparkSession, sparkContext: SparkContext): Unit = {
    import org.apache.spark.sql.functions.col
    import sparkSession.implicits._
    import org.apache.spark.sql.functions.{col, year}
    import org.apache.spark.sql.functions.count


    val tripdataDF = sparkSession.read.parquet("data/yellow_tripdata_2024-01.parquet")
    val zoneDF = sparkSession.read
      .option("header", "true")
      .csv("data/taxi_zone_lookup.csv")

    val airportZones = zoneDF
      .filter(col("Zone").isin("JFK Airport", "LaGuardia Airport", "Newark Airport"))
      .select(col("LocationID").cast("int").as("airport_id"))

    val airportIDs = airportZones
      .collect()
      .map(_.getAs[Int]("airport_id"))

    val result = tripdataDF
      .filter(
        col("DOLocationID").isin(airportIDs: _*)
      )
      .filter(  // samo voznje iz 2024. godine, kao u mejlu naglaseno
        year(col("tpep_pickup_datetime")) === 2024
      )
      .groupBy(col("PULocationID"))
      .count()
      .join(
        zoneDF.select(
          col("LocationID").cast("int").as("LocationID"),
          col("Zone"),
          col("Borough")
        ),
        col("PULocationID") === col("LocationID")
      )
      .select(col("Zone"), col("Borough"), col("count").cast("int").as("Ukupno Vožnji"))
      .orderBy(col("Ukupno Vožnji").desc)

    result.show()
  }

  private def query3(sparkSession: SparkSession, sparkContext: SparkContext): Unit = {
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions.{col, year}

    val tripdataDF = sparkSession.read.parquet("data/yellow_tripdata_2024-01.parquet")
    val zoneDF = sparkSession.read
      .option("header", "true")
      .csv("data/taxi_zone_lookup.csv")

    val dayZoneAvg = tripdataDF
      .withColumn("Datum", to_date(col("tpep_pickup_datetime")))
      .groupBy(col("Datum"), col("PULocationID"))
      .agg(avg(col("trip_distance")).as("Prosečna Dužina Vožnje"))

    val windowSpec = Window.partitionBy("Datum").orderBy(col("Prosečna Dužina Vožnje").desc)

    val result = dayZoneAvg
      .filter(  // samo voznje iz 2024. godine, kao u mejlu naglaseno
        year(col("Datum")) === 2024
      )
      .withColumn("rank", rank().over(windowSpec))
      .filter(col("rank") === 1)
      .drop("rank")
      .join(
        zoneDF.select(
          col("LocationID").cast("int").as("LocationID"),
          col("Zone"),
          col("Borough")
        ),
        col("PULocationID") === col("LocationID")
      )
      .select(col("Zone"), col("Borough"), col("Datum"), col("Prosečna Dužina Vožnje"))
      .orderBy(col("Datum"))

      result.show(30)
  }
}