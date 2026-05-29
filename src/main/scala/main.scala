import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    println(System.getProperty("java.version"))
    println(System.getProperty("java.home"))

    query1()
  }

  private def query1(): Unit = {
    val spark = SparkSession.builder().
      appName("Taxi Zone Lookup")
      .master("local[*]")
      .getOrCreate()

    val zoneLookup = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/taxi_zone_lookup.csv")

    val tripData = spark.read
      .parquet("data/yellow_tripdata_2024-01.parquet")

    println("Trip Data: " + tripData.count() + " rows")
    println("Zone Lookup: " + zoneLookup.count() + " rows")
  }
}