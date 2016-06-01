package common.ml

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD


class LogisticRegressionC(sc: SparkContext) {
  def fit(labelPoints: RDD[LabeledPoint]) = {

    val splitLabelPoints = labelPoints.randomSplit(Array(0.9, 0.1), 1212)

    val modelMLIB = new LogisticRegressionWithLBFGS()
      .setNumClasses(46)
      .run(splitLabelPoints(0))

    val predictionAndLabels = splitLabelPoints(1).map { case LabeledPoint(label, features) =>
      val prediction = modelMLIB.predict(features)
      (prediction, label)
    }
  }
}
