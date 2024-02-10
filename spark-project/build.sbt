ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "spark-project"
  )

libraryDependencies += "org.apache.spark" % "spark-core_2.13" % "3.5.0"
libraryDependencies += "org.apache.spark" % "spark-graphx_2.13" % "3.5.0"
