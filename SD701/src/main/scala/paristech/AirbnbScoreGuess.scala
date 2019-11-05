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
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.tuning.TrainValidationSplit

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
    "spark.driver.maxResultSize" -> "4g" 
    //,"spark.master" -> "local[*]"
    ))

  val spark = SparkSession
    .builder
    .config(conf)
    .appName("Airbnb Score Guess")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  // Import the dataFrame
  import spark.implicits._

  val datasetNew = spark.read.option("header", true)
    .option("multiline", true).csv("/tmp/airbnb/airbnb_paris2.csv")
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

  val subDataSetWithoutScore = datasetTypes.select("review_scores_location","review_scores_accuracy","review_scores_communication","review_scores_cleanliness","review_scores_value","review_scores_checkin"  ,"number_of_reviews", "accommodates", "bathrooms", "guests_included", "bedrooms", "beds",
    "price", "cleaning_fee", "bedrooms", "text", "property_type", "room_type", "neighbourhood", "review_scores_rating", "amenities")
    .withColumn("review_scores_rating", $"review_scores_rating".cast("Double"))
    .withColumn("review_scores_location", $"review_scores_location".cast("Double")).filter(!$"review_scores_location".isNull)
    .withColumn("review_scores_accuracy", $"review_scores_accuracy".cast("Double")).filter(!$"review_scores_accuracy".isNull)
    .withColumn("review_scores_communication", $"review_scores_communication".cast("Double")).filter(!$"review_scores_communication".isNull)
    .withColumn("review_scores_cleanliness", $"review_scores_cleanliness".cast("Double")).filter(!$"review_scores_cleanliness".isNull)
    .withColumn("review_scores_value", $"review_scores_value".cast("Double")).filter(!$"review_scores_value".isNull)
    .withColumn("review_scores_checkin", $"review_scores_checkin".cast("Double")).filter(!$"review_scores_checkin".isNull)
    .filter(!$"review_scores_rating".isNull).filter(!$"price".isNull).filter($"number_of_reviews" =!= 0)

    
 val subDataSet = subDataSetWithoutScore.withColumn("mean_score", ($"review_scores_rating"/10.0 +
     $"review_scores_location"+$"review_scores_communication"+$"review_scores_cleanliness"+$"review_scores_value"+$"review_scores_checkin")*10.0/7.0)   
      
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

  val countVectorizer = new CountVectorizer().setInputCol(stopWordRemover.getOutputCol).setOutputCol("TF").setMinDF(30)

  //    val dataSetHashing = new HashingTF().setInputCol("filtered").setOutputCol("features").transform(dataSetCountVect)
  //
  val IDF = new IDF().setInputCol(countVectorizer.getOutputCol).setOutputCol("tfidf")

  val vectorAssembler = new VectorAssembler().setInputCols(Array(
//      "review_scores_location","review_scores_location","review_scores_accuracy","review_scores_communication","review_scores_cleanliness","review_scores_value","review_scores_checkin"
//)).setOutputCol("features_assembled")
//
//      //      
    "tfidf",
    "room_onehot", "property_onehot",
    "neighbourhood_onehot", "features", "accommodates", "bathrooms", "guests_included", "bedrooms", "beds", "price", "number_of_reviews", "cleaning_fee")).setOutputCol("features_assembled")

  val rf = new RandomForestRegressor().setLabelCol("mean_score").setFeaturesCol("features_assembled")

  val lr = new LinearRegression()
    .setLabelCol("mean_score")
    .setFitIntercept(true)
    .setFeaturesCol(vectorAssembler.getOutputCol)
    .setPredictionCol("prediction")
    .setRegParam(0.1)
    .setElasticNetParam(0.0)
    .setEpsilon(10-8)
    .setMaxIter(1000)

  val pipeline: Pipeline = new Pipeline().setStages(Array( tokenizer, stopWordRemover, countVectorizer, IDF,
    indexerRoomType, indexerPropertyType, indexerNeighbourhoudType, oneHotEncorderCountry, vectorAssembler, lr))

  val Array(training, test) = dataSetFull.randomSplit(Array(0.9, 0.1), 999)

  val model = pipeline.fit(training)
  val dfWithSimplePredictions = model.transform(test)

  val evaluator = new RegressionEvaluator()
    .setLabelCol("mean_score")
    .setPredictionCol("prediction")
    .setMetricName("rmse")

  val rmse = evaluator.evaluate(dfWithSimplePredictions)

  dfWithSimplePredictions.select("mean_score", "prediction").show(100,false)
  
  println(s"Root Mean Squared Error (RMSE) on test data = $rmse")
  
  val paramGrid = new ParamGridBuilder().addGrid(lr.elasticNetParam, Array(1e-8, 1e-6, 1e-4, 1e-2))
                  .addGrid(lr.regParam, (0.01 to 0.3 by 0.01).toArray)
                  .addGrid(countVectorizer.minDF, (30.0 to 1500.0 by 30.0).toArray)
                  .build()

  val trainValidationSplit = new TrainValidationSplit().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setTrainRatio(0.7)
  
  val trainSplit = trainValidationSplit.fit(training)
  val testTransformed = trainSplit.transform(test)
  val mse = evaluator.evaluate(testTransformed)
  println(s"Root Mean Squared = ${mse}")
  
  trainSplit.save("/tmp/airbnb/model.sav")
  
}
