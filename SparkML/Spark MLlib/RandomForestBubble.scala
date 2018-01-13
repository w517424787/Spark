package mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RandomForestBubble {

  /**
    * 根据训练数据自动组合最佳参数
    *
    * @param trainingData
    * @param testData
    */
  def getBestParam(trainingData: RDD[LabeledPoint], testData: RDD[LabeledPoint]): Unit = {
    val evaluations = for (impurity <- Array("gini", "entropy"); maxDepth <- Array(5, 10, 15, 20, 25);
                           numTrees <- Array(50, 100, 200, 500, 1000)) yield {
      val model = RandomForest.trainClassifier(trainingData, 2, Map[Int, Int](),
        numTrees, "auto", impurity, maxDepth, 32)

      val metrics = getMetrics(model, testData)
      ((impurity, numTrees, maxDepth, 32), metrics.accuracy)
    }

    evaluations.sortBy(_._2).reverse.foreach(println)
  }

  /**
    * @param model    随机森林模型
    * @param testData 用于交叉验证的测试数据集
    **/
  def getMetrics(model: RandomForestModel, testData: RDD[LabeledPoint]): MulticlassMetrics = {
    //将交叉验证数据集的每个样本的特征向量交给模型预测,并和原本正确的目标特征组成一个tuple
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    //将结果交给MulticlassMetrics,其可以以不同的方式计算分配器预测的质量
    new MulticlassMetrics(labelAndPreds)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[2]").appName("RandomForestBubble").getOrCreate()

    val data1 = MLUtils.loadLibSVMFile(spark.sparkContext, "data/mllib/sample_libsvm_data.txt")

    import spark.implicits._
    //val rowData = spark.read.textFile("D:\\SparkApps2.2.1\\data\\test_all3.txt")
    val rowData = spark.read.textFile("D:\\SparkApps2.2.1\\data\\test_all4_1138.txt")
    val data = rowData.map { line =>
      val values = line.split(",").map(_.toDouble)
      //第一列作为label
      //其它为特征值
      val label = values.head
      //val feature = Vectors.dense(values.tail)
      //LabeledPoint(label, feature)
      LabeledPoint(label, Vectors.sparse(1138, (0 until 1138).toArray, values.tail))
    }

    //将数据拆分成训练集和测试集
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    getBestParam(trainingData.rdd, testData.rdd)

    //设置随机森林算法参数
    /*val numClasses = 2 //二分类
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 500 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini" //entropy
    val maxDepth = 15 //20
    val maxBins = 32 //32

    //训练模型
    val model = RandomForest.trainClassifier(trainingData.rdd, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    val labelAndPreds = testData.rdd.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println(s"Test Error = $testErr")

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(labelAndPreds)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)*/

    spark.stop()
  }
}
