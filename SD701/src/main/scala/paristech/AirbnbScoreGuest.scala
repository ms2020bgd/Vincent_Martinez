package paristech
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.OneHotEncoderEstimator
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression

import org.apache.spark.ml.regression.{ RandomForestRegressionModel, RandomForestRegressor }
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AirbnbScoreGuest extends App {

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
    //,"spark.master" -> "local[*]"
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
    .option("multiline", true).text("/home/martinez/git/ms2020bgd/SD701/airbnb/airbnb_paris.csv")
  dataset.printSchema()

  val toSave = dataset.withColumn("value", regexp_replace($"value", "\"\"", ""))

  val datasetcsv = spark.read.option("header", true)
    .option("multiline", true).csv("/home/martinez/git/ms2020bgd/SD701/airbnb/airbnb_paris.csv")

  datasetcsv.select("host_about").show(false)
  toSave.write.text("/home/martinez/git/ms2020bgd/SD701/airbnb/airbnb_paris2.csv")

  val datasetNew = spark.read.option("header", true)
    .option("multiline", true).csv("/home/martinez/git/ms2020bgd/SD701/airbnb/airbnb_paris2.csv")
  datasetNew.printSchema()
  datasetNew.select("interaction").show(false)

  // On recupere les colonnes qui nous interessent

  val datasetTypes = datasetNew.withColumn("accommodates", $"accommodates".cast("Int")).withColumn("accommodates", when($"accommodates".isNull, 2).otherwise($"accommodates"))
    .withColumn("bathrooms", $"bathrooms".cast("Int")).withColumn("bathrooms", when($"bathrooms".isNull, 0).otherwise($"bathrooms"))
    .withColumn("guests_included", $"guests_included".cast("Int")).withColumn("guests_included", when($"guests_included".isNull, 0).otherwise($"guests_included"))
    .withColumn("bedrooms", $"bedrooms".cast("Int")).withColumn("bedrooms", when($"bedrooms".isNull, 0).otherwise($"bedrooms"))
    .withColumn("beds", $"beds".cast("Int")).withColumn("beds", when($"beds".isNull, 0).otherwise($"beds"))
    .withColumn("price", regexp_replace($"price", "[$]", "")).withColumn("price", $"price".cast("Double"))
    .withColumn("cleaning_fee", regexp_replace($"cleaning_fee", "[$]", "")).withColumn("cleaning_fee", $"cleaning_fee".cast("Double")).withColumn("cleaning_fee", when($"cleaning_fee".isNull, 0).otherwise($"cleaning_fee"))
    .withColumn("name", when($"name".isNull, "").otherwise($"name"))
    .withColumn("summary", when($"summary".isNull, "").otherwise($"summary"))
    .withColumn("space", when($"space".isNull, "").otherwise($"space"))
    .withColumn("description", when($"description".isNull, "").otherwise($"description"))
    .withColumn("neighborhood_overview", when($"neighborhood_overview".isNull, "").otherwise($"neighborhood_overview"))
    .withColumn("text", concat_ws(" ", $"name", $"summary", $"space", $"description", $"neighborhood_overview"))
    .withColumn("amenities", regexp_replace($"amenities", "[{|}]", "")).withColumn("amenities", split($"amenities", ","))
    .withColumn("room_type", when($"room_type".isNull, "unknown").otherwise($"room_type"))
    .withColumn("property_type", when($"property_type".isNull, "unknown").otherwise($"property_type"))
    .withColumn("neighbourhood", when($"neighbourhood".isNull, "unknown").otherwise($"neighbourhood"))

  val subDataSet = datasetTypes.select("accommodates", "bathrooms", "guests_included", "bedrooms", "beds",
    "price", "cleaning_fee", "bedrooms", "text", "property_type", "room_type", "neighbourhood", "review_scores_rating", "amenities")
    .withColumn("review_scores_rating", $"review_scores_rating".cast("Double"))
    .filter(!$"review_scores_rating".isNull).filter(!$"price".isNull)

  // Get distinct tags array
  val amenities = subDataSet
    .flatMap(r ⇒ r.getAs[Seq[String]]("amenities"))
    .distinct()
    .collect()
    .sortWith(_ < _)

  val cvmData = new CountVectorizerModel(amenities)
    .setInputCol("amenities")
    .setOutputCol("sparseAmenities")
    .transform(subDataSet)

  val asDense = udf((v: Vector) ⇒ v.toDense)

  val dataSetFull = cvmData.withColumn("features", asDense($"sparseAmenities"))

  // Let's start the pipeline

  val indexerRoomType = new StringIndexer().setInputCol("room_type").setOutputCol("room_index").setHandleInvalid("skip")
  val indexerPropertyType = new StringIndexer().setInputCol("property_type").setOutputCol("property_index").setHandleInvalid("skip")
  val indexerNeighbourhoudType = new StringIndexer().setInputCol("neighbourhood").setOutputCol("neighbourhood_index").setHandleInvalid("skip")

  val oneHotEncorderCountry = new OneHotEncoderEstimator().setDropLast(false).setInputCols(Array(indexerRoomType.getOutputCol, indexerPropertyType.getOutputCol, indexerNeighbourhoudType.getOutputCol))
    .setOutputCols(Array("room_onehot", "property_onehot", "neighbourhood_onehot"))

  val tokenizer = new RegexTokenizer().setPattern("\\W+").setGaps(true).setInputCol("text").setOutputCol("tokens")
  val stopWordRemover = new StopWordsRemover().setInputCol(tokenizer.getOutputCol).setOutputCol("filtered")

  val countVectorizer = new CountVectorizer().setInputCol(stopWordRemover.getOutputCol).setOutputCol("TF").setMinDF(50)

  //    val dataSetHashing = new HashingTF().setInputCol("filtered").setOutputCol("features").transform(dataSetCountVect)
  //
  val IDF = new IDF().setInputCol(countVectorizer.getOutputCol).setOutputCol("tfidf")

  val vectorAssembler = new VectorAssembler().setInputCols(Array(
    "tfidf",
    "room_onehot", "property_onehot", "neighbourhood_onehot", "features",
    "accommodates", "bathrooms", "guests_included", "bedrooms", "beds",
    "price", "cleaning_fee")).setOutputCol("features_assembled")

  val rf = new RandomForestRegressor().setLabelCol("review_scores_rating").setFeaturesCol("features_assembled")

  val lr = new LinearRegression()
    .setLabelCol("review_scores_rating")
    .setFitIntercept(true)
    .setFeaturesCol("features_assembled")
    .setRegParam(0.2)
    .setMaxIter(100)

  val pipeline: Pipeline = new Pipeline().setStages(Array(tokenizer, stopWordRemover, countVectorizer, IDF, indexerRoomType, indexerPropertyType, indexerNeighbourhoudType, oneHotEncorderCountry, vectorAssembler, rf))

  val Array(training, test) = dataSetFull.randomSplit(Array(0.9, 0.1), 999)

  val model = pipeline.fit(training)
  val dfWithSimplePredictions = model.transform(test)

  val evaluator = new RegressionEvaluator()
    .setLabelCol("review_scores_rating")
    .setPredictionCol("prediction")
    .setMetricName("rmse")

  val rmse = evaluator.evaluate(dfWithSimplePredictions)
  println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

}
