package paristech

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Preprocessor extends App {

  val conf = new SparkConf().setAll(Map(
    "spark.scheduler.mode" -> "FIFO",
    "spark.speculation" -> "false",
    "spark.reducer.maxSizeInFlight" -> "48m",
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    "spark.kryoserializer.buffer.max" -> "1g",
    "spark.shuffle.file.buffer" -> "32k",
    "spark.default.parallelism" -> "12",
    "spark.sql.shuffle.partitions" -> "12",
    "spark.driver.maxResultSize" -> "2g"
    ,"spark.master" -> "local[*]"
    ))

  val spark = SparkSession
    .builder
    .config(conf)
    .appName("TP Spark : Trainer")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  // Import the dataFrame
  import spark.implicits._
  val dataset = spark.read.option("header", true)
    .option("multiline", true).text("./airbnb/airbnb_paris.csv")
  dataset.printSchema()

  val toSave = dataset.withColumn("value", regexp_replace($"value", "\"\"", ""))


   toSave.coalesce(1).write.text("./airbnb/airbnb_paris2.csv")
}
