# Apache_Spark using Scala
The Goal of this project to work/learn Apache Spark using Scala

I spent sufficient amount of time understanding Scala using these links:
    
    1. http://twitter.github.io/effectivescala/
    2. http://icl-f11.utcompling.com/links
    
I spent sufficient amount of time understanding Apache Spark using these links:
    1. http://spark.apache.org/docs/latest/index.html
    
1. First project within Spark/Scala module is to implement Multivariate Logistic Regression using Apache Spark.

#Logistic Regression using Apache Spark libraries.

1. I implemented multivariate Logistic Regression using Spark Libraries on Glass Identification Dataset.
2. Model should load a sample multiclass dataset,splits it into trainining and test 
   and uses LogisticRegressionWithLBFGS to fit a logistic regression model.
3. Trained model is evaluated against the test dataset and saved to disk.
4. Dataset is in csv format and not in standard LIBSVM format(default spark format)
5. Link for the UCI dataset: https://archive.ics.uci.edu/ml/datasets/Glass+Identification
  UCI glass dataset: which identifies the type of glass based on its components :
            ( -- 1 building_windows_float_processed
              -- 2 building_windows_non_float_processed
              -- 3 vehicle_windows_float_processed
              -- 4 vehicle_windows_non_float_processed  
              -- 5 containers
              -- 6 tableware
              -- 7 headlamps  ) 
  
