/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.uber.h3core.H3Core
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.flatspec.AnyFlatSpec

abstract class H3Spec extends AnyFlatSpec {
  Logger.getRootLogger.setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  it should "have extended function description" in {
    val functionDf = sparkSession.sql(s"DESC FUNCTION EXTENDED $functionName")
    val rows = functionDf.collect().map(_.getAs[String](0))
    assert(rows.exists(_.contains("Usage:")))
    assert(rows.exists(_.contains("Arguments:")))
    assert(rows.exists(_.contains("Examples:")))
  }

  protected def functionName: String

  protected val warehouseLocation: String = System.getProperty("user.dir") + "/target/"
  protected val resourceFolder: String = System.getProperty("user.dir") + "/../core/src/test/resources/"

  // scalastyle:off magic.number
  protected val invalidResolutions = Seq(-1, 16)
  // scalastyle:on magic.number

  protected lazy val sparkSession: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("h3SparkSqlTest")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .config("spark.sql.shuffle.partitions", "1")
    .withExtensions(new H3SqlExtensions().apply(_))
    .getOrCreate()

  protected def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    SparkSession.setActiveSession(sparkSession)
    val conf = SQLConf.get
    val (keys, values) = pairs.unzip
    val currentValues = keys.map { key =>
      if (conf.contains(key)) {
        Some(conf.getConfString(key))
      } else {
        None
      }
    }
    (keys, values).zipped.foreach { (k, v) =>
      if (SQLConf.staticConfKeys.contains(k)) {
        throw new Exception(s"Cannot modify the value of a static config: $k")
      }
      conf.setConfString(k, v)
    }
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => conf.setConfString(key, value)
        case (key, None) => conf.unsetConf(key)
      }
    }
  }

  protected lazy val h3: H3Core = H3Core.newInstance()
}
