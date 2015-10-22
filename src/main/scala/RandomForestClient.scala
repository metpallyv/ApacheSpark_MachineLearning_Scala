/**
 * Created by vardhaman on 10/20/15.
 */
package org.vam.spark
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
import scopt.OptionParser
import org.vam.spark.AbstractParams
import scala.collection.mutable

object RandomForestClient {
  // define default parameters
  case class Params(
                     numClasses: Int = 2,
                   inputFile: String = null,
                   numTrees:Int = 8,
                   //categoricalFeaturesInfo: Map[Int,Int] = Map[Int, Int](),
                   maxDepth: Int = 5,
                   featureSubsetStrategy: String = "auto",
                   impurity: String = "entropy",
                   maxBins: Int = 100,
                   checkpointDir: Option[String] = None,
                   checkpointInterval: Int = 10) extends AbstractParams[Params]

  def main (args: Array[String]) {
    val defaultParams = Params()
// get commandline parameters
    val parser = new OptionParser[Params]("RandomForest Spark Application") {
      head("Random Forest Apache Spark Implementation")
      opt[Int]("numClasses")
        .text(s"number of classes, default: ${defaultParams.numClasses}")
        .action((x, c) => c.copy(numClasses = x))
      opt[Int]("numTrees")
        .text(s"number of trees in ensemble, default: ${defaultParams.numTrees}")
        .action((x, c) => c.copy(numTrees = x))
      opt[Int]("maxDepth")
      .text(s"Maximum depth of the tree, default: ${defaultParams.maxDepth}")
      .action((x, c) => c.copy(maxDepth = x))
      opt[Int]("maxBins")
        .text(s"max number of bins, default: ${defaultParams.maxBins}")
        .action((x, c) => c.copy(maxBins = x))
      opt[String]("featureSubsetStrategy")
        .text(s"number of features to use, default: ${defaultParams.featureSubsetStrategy}")
        .action((x, c) => c.copy(featureSubsetStrategy = x))
      opt[Int]("impurity")
        .text(s"Impurity measure to be used, default: ${defaultParams.impurity}")
        .action((x, c) => c.copy(numTrees = x))
      opt[String]("checkpointDir")
        .text(s"checkpoint directory where intermediate node Id caches will be stored, " +
        s"default: ${
          defaultParams.checkpointDir match {
            case Some(strVal) => strVal
            case None => "None"
          }
        }")
        .action((x, c) => c.copy(checkpointDir = Some(x)))
      opt[Int]("checkpointInterval")
        .text(s"how often to checkpoint the node Id cache, " +
        s"default: ${defaultParams.checkpointInterval}")
        .action((x, c) => c.copy(checkpointInterval = x))
      arg[String]("<inputFile>")
        .required()
        .text("input File consisting of training data and test data")
        .action((x, c) => c.copy(inputFile = x))

    }
       parser.parse(args, defaultParams).map { params =>
        run(params)
      }.getOrElse {
        sys.exit(1)
    }
    }

  //running the spark application
  def run(params: Params) {
    // set up environment
    val conf = new SparkConf().setAppName("Spark Random Forest Application")

    //using spark kryo serializer
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[mutable.BitSet], classOf[RandomForestModel]))
      .set("spark.kryoserializer.buffer", "64m")

    val sc = new SparkContext(conf)

    //input file
    val data = sc.textFile(params.inputFile)
    //val data = sc.textFile(args(0))

//process each line and return label value and feature vector per line
    val rdd_data: RDD[LabeledPoint] = data.map{ line =>
      val parsed_line = line.replaceAll(",", " ").split(" ")
      LabeledPoint(parsed_line(parsed_line.length-1).toDouble ,Vectors.dense(parsed_line.slice(0,parsed_line.length-1).map(_.toDouble)))
    }

    // Split the data into training and test sets (30% held out for testing)
    val splits = rdd_data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    /*
    // Train a RandomForest model.
    val num_of_classes = 2
    val categoricalFeaturesInfo = Map[Int, Int]() //Empty categoricalFeaturesInfo indicates all features are continuous.
    val numTrees = 10
    val featureSubsetStrategy = "auto"
    val impurity = "entropy"
    val max_depth = 8
    val maxBins = 100 */

    val categoricalFeaturesInfo = Map[Int, Int]() //Empty categoricalFeaturesInfo indicates all features are continuous.

    //train the RandomForest classifier
    val model = RandomForest.trainClassifier(trainingData,params.numClasses,categoricalFeaturesInfo,params.numTrees,params.featureSubsetStrategy,params.impurity,params.maxDepth,params.maxBins)

  //predict values for test data, return predicited value along with label value
    val predictionsAndlabels = testData.map{ case LabeledPoint(label,features) =>
        val prediction = model.predict(features)
      (prediction,label)
    }

    /* Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionsAndlabels)
    val precision = sc.parallelize(metrics.precision)
    val pre = precision.toString*/
    val precision = predictionsAndlabels.filter(values => values._1 != values._2).count() / testData.count()
    println("Precison score is "+precision)
    //save the predicted values and label values as a file
    predictionsAndlabels.saveAsTextFile("RandomForest")

    sc.stop()

  }

}
