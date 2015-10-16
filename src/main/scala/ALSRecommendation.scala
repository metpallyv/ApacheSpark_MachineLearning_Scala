/**
 * Created by vardhaman on 10/12/15.
 */
package org.vam.spark

import scala.collection.mutable
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

object ALSClient {

  def main(args: Array[String]) {
    // set up environment
    val conf = new SparkConf().setAppName("ALS Recommendation System")
    //using spark kryo serializer
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[mutable.BitSet], classOf[Rating], classOf[MatrixFactorizationModel]))
      .set("spark.kryoserializer.buffer", "64m")

    val sc = new SparkContext(conf)
    // using checkpointing to make sure I dont get serialization error for huge no of iterations
    sc.setCheckpointDir("MovieRecommendationCheckPoint")
    
    // Load and parse the movies rating file and movies mapping file
    //movies rating file consists of userid,movieid,rating,timestamp
    //movies mapping file consists of movieid,movie name,genre
    val ratings_file = sc.textFile(args(0))
    val movie_mapping_file = sc.textFile(args(1))

    //get the list of users for whom u want to make the recommendation of top 25 movies
    // file has every line with userid, for whom we need to recommend movies
    val users_file = sc.textFile(args(2))

    //I use collect method so that I can make sure i got serialization error
    val users = users_file.map { line => line.stripLineEnd.toInt
    }.collect()

    //no of hidden features
    val features = 10

    //the regularization parameter
    val lambda = 0.02
    //no of steps before convergence
    val numIterations = 100

    // load rating file and build ratings input RDD of (user, product, rating) triples
    val ratings = ratings_file.map { line =>
      val fields = line.split(",")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }.cache()

    //ratings grouped by each user id. userid,[rating1,...]
    val ratingsGroupedByUser = ratings.map(rating => (rating.user, rating)).groupByKey().persist()

    //user movie_id pair
    val userProductsPair = ratings.map(rating => (rating.user, rating.product))

    // load movie mapping file and build a Map of (movieid,MovieName) pair
    val movies = movie_mapping_file.map { line =>
      val fields = line.split(",")
      (fields(0).toInt, fields(1))
    }.collect().toMap

    //train the model
    val model = {
      new ALS().setRank(features).setLambda(lambda).setIterations(numIterations).setImplicitPrefs(false).run(ratings)
    }
    //recommended movie name along with user id for every movie
    val movies_name = users.map { user =>

      //get all the rated products for each user. To list is imp as I use it for filtering
      val rated_products: List[Int] = ratingsGroupedByUser.lookup(user)(0).map(rat => rat.product).toList

      //filter out the movies which have not been rated by the user
      val unrated_products = sc.parallelize(movies.keys.filter(!rated_products.contains(_)).toSeq)

      //get the top 25 predicted movies based on the rating for all the unrated movies
      val recommendation = model.predict(unrated_products.map(product => (user, product))).sortBy(-_.rating).take(25)

      //recommend the movie name along with the user id. Need user id as results are stored in distributed manner
      val movies_name = sc.parallelize(recommendation.map(r => (user, movies(r.product))))

      movies_name
    }
    //save recommended movies as a file in HDFS
   // movies_name(0).saveAsTextFile("ALSMovieRecommendation")
        //save recommended movies as a file in HDFS
    var i = 0
    val final_value= movies_name.foreach{rdd=>
    rdd.saveAsTextFile("ALSMovieRecommend/"+i)
      i = i+1
    }
    sc.stop()
  }
}
