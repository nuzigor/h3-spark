/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class H3Spec extends AnyFlatSpec {
  Logger.getRootLogger.setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  protected val warehouseLocation: String = System.getProperty("user.dir") + "/target/"
  protected val resourceFolder: String = System.getProperty("user.dir") + "/../core/src/test/resources/"

  protected lazy val sparkSession: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("h3SparkSqlTest")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .config("spark.sql.shuffle.partitions", "1")
    .withExtensions(new H3SqlExtensions().apply(_))
    .getOrCreate()
}
