/*
 * Copyright 2021 Igor Nuzhnov
 *
 * SPDX-License-Identifier: Apache-2.0
 */

ThisBuild / organization := "io.github.nuzigor"
ThisBuild / organizationName := "nuzigor"
ThisBuild / description := "Brings H3 - Hexagonal hierarchical geospatial indexing system support to Apache Spark SQL."
ThisBuild / homepage := Some(url("https://github.com/nuzigor/h3-spark"))
ThisBuild / licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / versionScheme := Some("semver-spec")
ThisBuild / startYear := Some(2021)
ThisBuild / developers := List(Developer("nuzigor", "Igor Nuzhnov", "nuzhnov@gmail.com", url("https://github.com/nuzigor")))

headerLicense := Some(HeaderLicense.ALv2("2021", "Igor Nuzhnov", HeaderLicenseStyle.SpdxSyntax))

ThisBuild / scmInfo := Some(ScmInfo(url("https://github.com/nuzigor/h3-spark"), "scm:git@github.com:nuzigor/h3-spark.git"))

ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishMavenStyle := true
ThisBuild / publishTo := sonatypePublishToBundle.value
ThisBuild / sonatypeCredentialHost := "s01.oss.sonatype.org"

ThisBuild / scalaVersion := "2.12.17"
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision
ThisBuild / scalacOptions ++= List("-Ywarn-unused-import", "-Ywarn-adapted-args", "-deprecation")

import ReleaseTransformations._
releasePublishArtifactsAction := PgpKeys.publishSigned.value
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepCommand("publishSigned"),
  releaseStepCommand("sonatypeBundleRelease"),
  setNextVersion,
  commitNextVersion
)

lazy val root = (project in file("."))
  .settings(name := "h3-spark")

val sparkVersion = "3.5.2"
val h3Version = "4.1.1"
val jtsVersion = "1.19.0"
val scalatestVersion = "3.2.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "com.uber" % "h3" % h3Version,
  "org.locationtech.jts" % "jts-core" % jtsVersion,
  "org.scalatest" %% "scalatest" % scalatestVersion % "test"
)

Test / parallelExecution := false
Test / logBuffered := false
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
Test / fork := true
