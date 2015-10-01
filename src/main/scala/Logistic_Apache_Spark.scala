/**
 * Created by vardhaman on 09/05/15.
 */
package org.vam.spark

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.Seconds
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.Vectors

/**
 * Sample Driver for Logistic Regression using Spark
 */
object Logistic_Apache_Spark {

  /* The below function does data pre-processing for a csv formatted file
   and returns a Labeled point along with vector for the feature */

  def parseLine(line: String): LabeledPoint =  {
    val parsed_line = line.replaceAll(",", " ")
    val parts = parsed_line.split(" ")
    LabeledPoint(parts(parts.length-1).toDouble-1, Vectors.dense(parts.slice(1,parts.length-1).map(_.toDouble)))

  }

  //This is the main code
  def main(args: Array[String]){

    /* Creating a new Spark conf */
    val conf = new SparkConf().setAppName("Logistic regression using Spark")
    val sc = new SparkContext(conf)
    val data = sc.textFile(args(0))
    /* Creation of labelled point for the data */
    val parsed = data.map(parseLine)
    /*val scaler1 = new StandardScaler(withMean = true, withStd = true).fit(parsed.map { case LabeledPoint(label,features) =>
       features}) */

    /* Applying zero-mean normalization to the data points */
    val scaler1 = new StandardScaler(withMean = true, withStd = true).fit(parsed.map(x => x.features))
    val data2 = parsed.map(x => LabeledPoint(x.label, scaler1.transform(Vectors.dense(x.features.toArray))))


    // Split data into training (80%) and test (20%).
    val splits = data2.randomSplit(Array(0.8,0.2))
    val training = splits(0).cache()
    val test = splits(1).cache()

    // Run training algorithm to build the model
    val num_features = 9
    val num_iter = 100
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(7)
      .run(training)

    // Compute raw scores on the test set.
    val predictionsAndLabels = test.map { case LabeledPoint(label,features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionsAndLabels)
    val precision = metrics.precision

    //Save the predicted value with original label value to a hdfs file

    predictionsAndLabels.saveAsTextFile("Logistic_output")
    //println(predictionsAndLabels)
    //precision .saveAsHadoopFiles("Logistic", "suffix")
    println("Precision ="+ precision)
  }



}