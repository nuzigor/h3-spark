/*
 * Copyright 2021 Igor Nuzhnov
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.spark.sql.h3.functions._
import org.apache.spark.sql.functions.column
import org.apache.spark.sql.internal.SQLConf

class FromWktSpec extends H3Spec {
  it should "convert WKT point to h3" in {
    val df = sparkSession.sql(s"SELECT $functionName('POINT (-0.2983396 35.8466667)', 10)")
    val h3 = df.first().getAs[Long](0)
    assert(h3 === 0x8a382ed85c37fffL)
  }

  it should "return null for empty WKT point" in {
    val df = sparkSession.sql(s"SELECT $functionName('POINT EMPTY', 10)")
    assert(df.first().isNullAt(0))
  }

  it should "return null for invalid WKT" in {
    val df = sparkSession.sql(s"SELECT $functionName('bla bla', 10)")
    assert(df.first().isNullAt(0))
  }

  it should "return null for null WKT" in {
    val df = sparkSession.sql(s"SELECT $functionName(null, 10)")
    assert(df.first().isNullAt(0))
  }

  it should "return null for null resolution" in {
    val df = sparkSession.sql(s"SELECT $functionName('POINT (-0.2983396 35.8466667)', null)")
    assert(df.first().isNullAt(0))
  }

  it should "support compiled function" in {
    import sparkSession.implicits._
    val df = Seq(("POINT (-0.2983396 35.8466667)", 1)).toDF("wkt", "id")
    val resolution = 10
    val result = df.select(h3_from_wkt(column("wkt"), resolution).alias("h3"))
    val h3 = result.first().getAs[Long](0)
    assert(h3 === 0x8a382ed85c37fffL)
  }

  it should "return null for invalid resolution" in {
    invalidResolutions.foreach { resolution =>
      val df = sparkSession.sql(s"SELECT $functionName('POINT (-0.2983396 35.8466667)', $resolution)")
      assert(df.first().isNullAt(0))
    }
  }

  it should "fail for invalid parameters when ansi enabled" in {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      Seq(s"SELECT $functionName('POINT (-0.2983396 35.8466667)', -1)", s"SELECT $functionName('bla bla', 10)")
        .foreach { script =>
          assertThrows[Throwable] {
            sparkSession.sql(script).collect()
          }
        }
    }
  }

  protected override def functionName: String = "h3_from_wkt"
}
