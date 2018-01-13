package ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object LogisticRegressionBubble {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[2]").appName("LogisticRegressionBubble").getOrCreate()

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

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val lr = new LogisticRegression()
      .setMaxIter(100)
      .setRegParam(0.2)
      .setElasticNetParam(0.8)

    //数据拆分成训练集和测试集
    val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2), 4000)

    // Chain indexers and logistic regression in a Pipeline.
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, lr, labelConverter))

    //通过pipeline训练出模型
    val model = pipeline.fit(trainingData)

    //用测试数据进行预测
    val predictions = model.transform(testData)
    predictions.cache()

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)

    println(s"accuracy = $accuracy")

    spark.stop()

  }
}
