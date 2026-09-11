import org.apache.spark.sql.SparkSession

object Day3SparkApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day3 Spark First Application")
      .master("local[4]")
      .getOrCreate()

    val sc = spark.sparkContext

    println("Spark Application Started")
    println("Application Name: " + sc.appName)
    println("Master: " + sc.master)

    val data = sc.textFile("data/input.txt")

    println("----- File Contents -----")
    data.collect().foreach(println)

    spark.stop()
  }
}
