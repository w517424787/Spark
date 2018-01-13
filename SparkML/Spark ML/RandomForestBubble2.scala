package ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Dataset, SparkSession}

object RandomForestBubble2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[2]").appName("RandomForestBubble2").getOrCreate()

    import spark.implicits._
    //val rowData = spark.read.textFile("D:\\SparkApps2.2.1\\data\\test_all3.txt")
    val rowData = spark.read.textFile("D:\\SparkApps2.2.1\\data\\test_all4_1138.txt")
    val data = rowData.map { line =>
      val values = line.split(",").map(_.toDouble)
      //第一列作为label
      //其它为特征值
      val label = values.head
      val feature = Vectors.dense(values.tail)
      (label, feature)
    }.toDF("label", "features")

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(32)
      .fit(data)

    //拆分训练集和测试集
    val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2), 4000)

    // Train a RandomForest model.
    // 设置RandomForest参数
    val randomForest = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(500)
      .setMaxDepth(15)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // Chain indexers and forest in a Pipeline.
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, randomForest, labelConverter))

    // Train model. This also runs the indexers.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)
    predictions.cache()

    // Select example rows to display.
    //predictions.select("predictedLabel", "label", "features").where("predictedLabel != label").show()
    //predictions.select("predictedLabel", "label", "features").filter($"predictedLabel" =!= $"label").show()
    //predictions.show(20)

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)

    val evaluator1 = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("weightedRecall")

    val weightedRecall = evaluator1.evaluate(predictions)

    println(s"accuracy = $accuracy")
    println(s"weightedRecall = $weightedRecall")

    //ROC
    val binaryEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setRawPredictionCol("prediction")
    println(s"areaUnderROC =" + binaryEvaluator.setMetricName("areaUnderROC").evaluate(predictions))
    println(s"areaUnderPR = " + binaryEvaluator.setMetricName("areaUnderPR").evaluate(predictions))

    spark.stop()
  }
}