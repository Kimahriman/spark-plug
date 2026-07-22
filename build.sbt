ThisBuild / organization := "com.github.kimahriman"
ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.13.16"

lazy val sparkVersion = "4.0.0"

lazy val root = (project in file("."))
  .settings(
    name := "spark-plug",
    baseDirectory := baseDirectory.value / "plugin",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-connect" % sparkVersion % Provided,
    )
  )
