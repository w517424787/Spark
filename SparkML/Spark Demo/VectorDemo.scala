package mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{Matrices, Vectors}
import org.apache.spark.sql.SparkSession

object VectorDemo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[2]").appName("VectorDemo").getOrCreate()

    val vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
    vector.toArray.foreach(println)
    val vector2 = Vectors.dense(Array(1.0, 0.0, 3.0))
    vector2.toArray.foreach(println)
    val vector3 = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
    vector3.toArray.foreach(println)
    val vector4 = Vectors.sparse(3, Array((0, 1.0), (2, 3.0)))
    vector4.toArray.foreach(println)

    val pos = LabeledPoint(1.0, Vectors.dense(Array(1.0, 0.0, 3.0)))
    val neg = LabeledPoint(1.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))

    val dmMatrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
    /** {{{
      *   1.0 0.0 4.0
      *   0.0 3.0 5.0
      *   2.0 0.0 6.0
      * }}}
      * is stored as `values: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]`,
      * `colPointers=[0, 2, 3, 6]`,`rowIndices=[0, 2, 1, 0, 1, 2]`
      * 先根据colPointers数组中的元素对values值进行切块，同时也对应的列顺序，数组中的元素对应的是values数组的index
      * 根据上面的colPointers对数据进行切块为((1.0,2.0),(3.0),(4.0,5.0,6.0))，然后在根据rowIndices对数据按行进行移动
      * */
    val spMatrix = Matrices.sparse(3, 3, Array(0, 2, 3, 6), Array(0, 2, 1, 0, 1, 2), Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
    println(spMatrix)
  }
}
