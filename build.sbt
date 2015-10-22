name := "ApacheSpark_Scala"

version := "1.0"

scalaVersion := "2.10"


libraryDependencies += "org.apache.spark" %"spark-core_2.10" %"1.4.1"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.4.1"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.4.1"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.2.0"
