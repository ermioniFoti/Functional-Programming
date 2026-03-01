ThisBuild / version := "0.1.0-SNAPSHOT"

//THIS IS FOR LOCAL MODE
ThisBuild / scalaVersion := "2.13.8"

//THIS IS FOR CLUSTER MODE
//ThisBuild / scalaVersion := "2.11.8"

lazy val root = (project in file("."))
  .settings(
    name := "INF424_First_Assignment"
  )

//THIS IS FOR LOCAL MODE
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql" % "3.5.1" )

//THIS IS FOR CLUSTER MODE
//libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % "2.3.1",
//  "org.apache.spark" %% "spark-sql" % "2.3.1",
//  "org.apache.hadoop" % "hadoop-client" % "3.1.1")


