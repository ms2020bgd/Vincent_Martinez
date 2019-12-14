package paristech

import java.util.Locale

import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.OneHotEncoderEstimator
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.regression.GeneralizedLinearRegression
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.tuning.TrainValidationSplit
import org.apache.spark.ml.tree.impl.RandomForest
import org.apache.spark.ml.tree.impl.RandomForest
import org.apache.spark.ml.regression.RandomForestRegressionModel
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object AirbnbScoreGuess extends App {

  val conf = new SparkConf().setAll(Map(
    "spark.scheduler.mode" -> "FIFO",
    "spark.speculation" -> "false",
    "spark.reducer.maxSizeInFlight" -> "48m",
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    "spark.kryoserializer.buffer.max" -> "1g",
    "spark.shuffle.file.buffer" -> "32k",
    "spark.default.parallelism" -> "12",
    "spark.sql.shuffle.partitions" -> "12",
    "spark.driver.maxResultSize" -> "4g", "spark.master" -> "local[*]"))

  val spark = SparkSession
    .builder
    .config(conf)
    .appName("Airbnb Score Guess")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  // Import the dataFrame
  import spark.implicits._

  val datasetNew = spark.read.option("header", true)
    .option("multiline", true).csv("airbnb/airbnb_paris2.csv")
  datasetNew.printSchema()
  //datasetNew.select("number_of_reviews").show(10000,false)

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
    .withColumn("number_of_reviews", $"number_of_reviews".cast("Int"))

  val subDataSetWithoutScore = datasetTypes.select("review_scores_location", "review_scores_accuracy", "review_scores_communication", "review_scores_cleanliness", "review_scores_value", "review_scores_checkin", "review_scores_rating", "number_of_reviews", "accommodates", "bathrooms", "guests_included", "bedrooms", "beds",
    "price", "weekly_price", "cleaning_fee", "text", "property_type", "room_type", "neighbourhood", "amenities")
    .withColumn("review_scores_rating", $"review_scores_rating".cast("Double")).filter(!$"review_scores_rating".isNull)
    .withColumn("review_scores_location", $"review_scores_location".cast("Double")).filter(!$"review_scores_location".isNull)
    .withColumn("review_scores_accuracy", $"review_scores_accuracy".cast("Double")).filter(!$"review_scores_accuracy".isNull)
    .withColumn("review_scores_communication", $"review_scores_communication".cast("Double")).filter(!$"review_scores_communication".isNull)
    .withColumn("review_scores_cleanliness", $"review_scores_cleanliness".cast("Double")).filter(!$"review_scores_cleanliness".isNull)
    .withColumn("review_scores_value", $"review_scores_value".cast("Double")).filter(!$"review_scores_value".isNull)
    .withColumn("review_scores_checkin", $"review_scores_checkin".cast("Double")).filter(!$"review_scores_checkin".isNull)
    .withColumn("weekly_price", $"weekly_price".cast("Double")).withColumn("weekly_price", when($"weekly_price".isNull, $"price" * 7).otherwise($"weekly_price"))
    .filter(!$"weekly_price".isNull).filter($"number_of_reviews" =!= 0)

  // Try with a classifier

  // Let's say that score >= 99, 1, between 97.5 and 99,  95 and 97,5, 90 and 95, 75 to 90 and less

  def convertMeanScore(score: Double): Int = {
    val minus = 100 - score
    if (minus <= 1)
      return 0
    else if (minus <= 2.5)
      return 1
    else if (minus <= 5)
      return 2
    else if (minus <= 10)
      return 3
    else if (minus <= 25)
      return 4

    return 5

  }

  val convertScore = udf(convertMeanScore _)

  val Array(q95) = subDataSetWithoutScore.stat.approxQuantile("weekly_price", Array(0.95), 0)
  // Remove too high value and 0 value
  val subDataFilteredPrice = subDataSetWithoutScore.filter($"weekly_price" > 0).filter($"weekly_price" < q95)

  val subDataSet = subDataFilteredPrice.withColumn(
    "mean_score",
    ($"review_scores_rating" / 10.0 + $"review_scores_location" + $"review_scores_communication" + $"review_scores_cleanliness" + $"review_scores_value" + $"review_scores_checkin" + $"review_scores_accuracy") * 10.0 / 7.0)
    .withColumn("score_cat", convertScore($"mean_score"))

  // Get distinct tags array+ $"review_scores_rating"+
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
  val stopWordRemoverFr = new StopWordsRemover().setInputCol(stopWordRemover.getOutputCol).setOutputCol("filteredFr").setLocale(Locale.FRENCH.toString())

  val countVectorizer = new CountVectorizer().setInputCol(stopWordRemoverFr.getOutputCol).setOutputCol("TF").setVocabSize(200)

  //
  val IDF = new IDF().setInputCol(countVectorizer.getOutputCol).setOutputCol("tfidf")

  val vectorAssemblerMeanScore = new VectorAssembler().setInputCols(Array(
    "tfidf",
    "room_onehot", "property_onehot",
    "neighbourhood_onehot", "features",
    "accommodates",
    "bathrooms",
    "guests_included",
    "bedrooms",
    "beds",
    "weekly_price",
    // "mean_score",
    "number_of_reviews",
    "cleaning_fee"
  //
  /*"review_scores_rating",
    "review_scores_location",
    "review_scores_communication",
    "review_scores_cleanliness",
    "review_scores_value",
    "review_scores_checkin"*/

  )).setOutputCol("features_assembled_mean_score")

  val vectorAssemblerWeeklyPrice = new VectorAssembler().setInputCols(Array(
    "tfidf",
    "room_onehot",
    "property_onehot",
    "neighbourhood_onehot",
    "features",
    "accommodates",
    "bathrooms",
    "guests_included",
    "bedrooms",
    "beds",
    //"weekly_price",
    "mean_score",
    "number_of_reviews",
    "cleaning_fee")).setOutputCol("features_assembled_weekly")

  val rfMeanScore = new RandomForestRegressor().setLabelCol("mean_score").setPredictionCol("mean_score_prect_rf").setFeaturesCol(vectorAssemblerMeanScore.getOutputCol)

  val rfCatScore = new RandomForestClassifier().setLabelCol("score_cat").setPredictionCol("score_cat_prect").setFeaturesCol(vectorAssemblerMeanScore.getOutputCol)

  val rfPrice = new RandomForestRegressor().setLabelCol("weekly_price").setPredictionCol("weekly_price_prect").setFeaturesCol(vectorAssemblerWeeklyPrice.getOutputCol)

  val lrMeanScore = new LinearRegression()
    .setLabelCol("mean_score")
    .setFeaturesCol(vectorAssemblerMeanScore.getOutputCol)
    .setPredictionCol("mean_score_prect")
    .setRegParam(0.01)
    .setElasticNetParam(0.8)
    .setTol(1e-8)
    .setMaxIter(1000)

  val pipeline: Pipeline = new Pipeline().setStages(Array(tokenizer, stopWordRemover, stopWordRemoverFr, countVectorizer, IDF,
    indexerRoomType, indexerPropertyType, indexerNeighbourhoudType, oneHotEncorderCountry, vectorAssemblerMeanScore, vectorAssemblerWeeklyPrice, rfMeanScore, rfCatScore, rfPrice, lrMeanScore))

  val Array(training, test) = dataSetFull.randomSplit(Array(0.9, 0.1), 999)

  val model = pipeline.fit(training)

  val dfWithSimplePredictions = model.transform(test)

  val evaluatorWeekly = new RegressionEvaluator()
    .setLabelCol("weekly_price")
    .setPredictionCol("weekly_price_prect")
    .setMetricName("rmse")

  val evaluatorRfMean = new RegressionEvaluator()
    .setLabelCol("mean_score")
    .setPredictionCol("mean_score_prect_rf")
    .setMetricName("rmse")

  val evaluatorLrMean = new RegressionEvaluator()
    .setLabelCol("mean_score")
    .setPredictionCol("mean_score_prect")
    .setMetricName("rmse")

  var rmse = evaluatorWeekly.evaluate(dfWithSimplePredictions)
  //dfWithSimplePredictions.select("weekly_price", "weekly_price_prect").show(100, false)

  println("The performance for the regression 'rfPrice' :" + rmse)

  rmse = evaluatorRfMean.evaluate(dfWithSimplePredictions)
  println("The performance for the regression 'rfMean' :" + rmse)
  
  
  rmse = evaluatorLrMean.evaluate(dfWithSimplePredictions)
  println("The performance for the regression 'lrMean' :" + rmse)
  
/*
  val paramGrid = new ParamGridBuilder()
    .addGrid(lrMeanScore.elasticNetParam, Array(1e-2, 1e-1, 0.5, 0.8))
    .addGrid(lrMeanScore.regParam, (0.1 to 0.3 by 0.05).toArray)
    //.addGrid(rf., (0.1 to 0.3 by 0.05).toArray)
    //.addGrid(countVectorizer.vocabSize, (50 to 200 by 50).toArray)
    .build()

  val evaluator = new RegressionEvaluator()
    .setLabelCol("mean_score")
    .setPredictionCol("mean_score_prect")
    .setMetricName("rmse")
  //dfWithSimplePredictions.select("weekly_price", "prediction").write.csv("airbnb/results.csv")

  val trainValidationSplit = new TrainValidationSplit().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setTrainRatio(0.7)

  val trainSplit = trainValidationSplit.fit(training)
  val testTransformed = trainSplit.transform(test)
  val mse = evaluator.evaluate(testTransformed)
  println(s"Root Mean Squared = ${mse}")

  println(s"Root Mean Squared Error (RMSE) on test data = $rmse")*/

  val f1Evaluator = new MulticlassClassificationEvaluator().setLabelCol("score_cat").setPredictionCol("score_cat_prect").
    setMetricName("f1")
  val f1 = f1Evaluator.evaluate(dfWithSimplePredictions)

  val precisionEvaluator = new MulticlassClassificationEvaluator().setLabelCol("score_cat").setPredictionCol("score_cat_prect").
    setMetricName("weightedPrecision")
  val precision = precisionEvaluator.evaluate(dfWithSimplePredictions)

  val recallEvaluator = new MulticlassClassificationEvaluator().setLabelCol("score_cat").setPredictionCol("score_cat_prect").
    setMetricName("weightedRecall")
  val recall = recallEvaluator.evaluate(dfWithSimplePredictions)

  println(s"F1 precision = ${f1}")
  println(s"Recall = ${recall}")
  println(s"Precision = ${precision}")

  dfWithSimplePredictions.groupBy("score_cat", "score_cat_prect").count.show()

  /*  trainSplit.save("airbnb/model_fitted.sav")*/

}
