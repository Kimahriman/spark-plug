ThisBuild / organization := "com.github.kimahriman"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.14"

lazy val sparkVersion = "4.0.0"

lazy val root = (project in file("plugin"))
  .settings(
    name := "spark-connect-proxy",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-connect" % sparkVersion % Provided,
    )
  )

// autoScalaLibrary := false
// crossPaths := false
publishArtifact := false  // Don't release the root project
publish / skip := true