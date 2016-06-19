name := "spark-common"

organization := "com.spark-learning"

version := "0.0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.5.2"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "1.5.2"

libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.2.0"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test"

libraryDependencies += "junit" % "junit" % "4.10" % "test"

parallelExecution in Test:= false
