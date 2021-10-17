/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

// Global / onChangedBuildSource := ReloadOnSourceChanges

ThisBuild / organization := "com.nuzigor"
ThisBuild / scalaVersion := "2.12.15"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / name         := "h3-spark"
ThisBuild / licenses     := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / developers   := List(
  Developer(
    "nuzigor",
    "Igor Nuzhnov",
    "nuzhnov@gmail.com",
    url("https://github.com/nuzigor")
  )
)

/*
lazy val root = (project in file("."))
  .settings(
    name := "h3-spark"
  )
*/

val sparkVersion = "3.1.2"
val h3Version = "3.7.1"
val jtsVersion = "1.18.2"
val scalatestVersion = "3.2.10"
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
