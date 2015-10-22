# MachineLearning_Spark_Scala
The Goal of this project run machine learning algorithms on distributed file system(HDFS Yarn) using Apache Spark and Scala. 

I spent sufficient amount of time understanding Scala using these links:
    
    1. http://twitter.github.io/effectivescala/
    2. http://icl-f11.utcompling.com/links
    
I spent sufficient amount of time understanding Apache Spark using these links:

    1. http://spark.apache.org/docs/latest/index.html
    2. https://www.safaribooksonline.com/library/view/learning-spark/9781449359034/ch01.html
    
I am sure you will find lots of material on internet on distributed file system and the need for dfs, especially in Machine Learning and NLP.

Here are my projects:(I keep adding new projects)

1. First project within Spark/Scala module is to implement Multivariate Classification using Logistic Regression 
2. Second project with Spark/Scala module is to implement Movie Recommendation using Spark Mllib ALS algorithm.
3. Third project with Spark/Scala module is to implement Classification task using Ensemble methods(Random Forest)

#Multivariate Classification using Logistic Regression

1. I implemented multivariate Classification - Logistic Regression using Spark Libraries on Glass Identification Dataset.
2. Model should load a sample multiclass dataset,split it into trainining and test 
   and uses LogisticRegressionWithLBFGS to fit a logistic regression model to classify the type of glass.
3. Trained model is evaluated against the test dataset and saved to disk.
4. Dataset is in csv format and not in standard LIBSVM format(default spark format)
5. Apply z-mean normalization to the data.
6. Link for the UCI dataset: https://archive.ics.uci.edu/ml/datasets/Glass+Identification
  UCI glass dataset: which identifies the type of glass based on its components :
            ( -- 1 building_windows_float_processed
              -- 2 building_windows_non_float_processed
              -- 3 vehicle_windows_float_processed
              -- 4 vehicle_windows_non_float_processed  
              -- 5 containers
              -- 6 tableware
              -- 7 headlamps  ) 
  
#Movie Recommendation using ALS(Alternating Least Sqaure algorithm)

1. I implmented distributed version for Movie Recommendation using Apache Spark ALS algorithm as it was taking 3 hours to train a model with 1 Million ratings on my laptop/single machine. Running it on Apache Spark took like 15 minutes. Boom! What a performance improvement

2. Refer to Movie recommendation project for more details on what the project is about and run.txt for more steps on how to run the code.
https://github.com/metpallyv/MovieRecommendation

#Classification task using Ensemble methods(Random Forest)

1. Goal of this model is to build binary/multivariate classifers using Ensemble methods such as Random forest. Random forest tend to perform better most of other classifiers.
2. I run the Random Forest model on:

            1. Spambase dataset, whose task is to classify an email as "Spam" or "Not Spam".
            https://archive.ics.uci.edu/ml/datasets/Spambase
            
            2. UCI glass dataset mentioned above.
