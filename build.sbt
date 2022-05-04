/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

ThisBuild / organization := "io.github.nuzigor"
ThisBuild / organizationName := "nuzigor"
ThisBuild / description := "Brings H3 - Hexagonal hierarchical geospatial indexing system support to Apache Spark SQL."
ThisBuild / homepage := Some(url("https://github.com/nuzigor/h3-spark"))
ThisBuild / licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / versionScheme := Some("semver-spec")
ThisBuild / version      := "0.8.0"

ThisBuild / developers := List(
  Developer(
    "nuzigor",
    "Igor Nuzhnov",
    "nuzhnov@gmail.com",
    url("https://github.com/nuzigor")
  )
)

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/nuzigor/h3-spark"),
    "scm:git@github.com:nuzigor/h3-spark.git"
  )
)

ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://s01.oss.sonatype.org/"
  if (isSnapshot.value) {
    Some("snapshots" at nexus + "content/repositories/snapshots")
  }
  else {
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
  }
}
ThisBuild / publishMavenStyle := true

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "h3-spark"
  )

val sparkVersion = "3.2.1"
val h3Version = "3.7.2"
val jtsVersion = "1.18.2"
val scalatestVersion = "3.2.11"
val scalacheckVersion = "1.15.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "com.uber" % "h3" % h3Version,
  "org.locationtech.jts" % "jts-core" % jtsVersion,
  "org.scalatest" %% "scalatest" % scalatestVersion % "test",
)

Test / parallelExecution := false
Test / logBuffered := false
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
Test / fork := true
