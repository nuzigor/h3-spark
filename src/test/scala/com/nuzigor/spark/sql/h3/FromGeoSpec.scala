/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.spark.sql.h3.functions._
import org.apache.spark.sql.functions.column
import org.apache.spark.sql.internal.SQLConf

class FromGeoSpec extends H3Spec {
  it should "convert point to h3" in {
    val df = sparkSession.sql(s"SELECT $functionName(35.8466667d, -0.2983396d, 10)")
    val h3 = df.first().getAs[Long](0)
    assert(h3 === 0x8A382ED85C37FFFL)
  }

  it should "return null for null lat" in {
    val df = sparkSession.sql(s"SELECT $functionName(null, -0.2983396d, 10)")
    assert(df.first().isNullAt(0))
  }

  it should "return null for null lng" in {
    val df = sparkSession.sql(s"SELECT $functionName(-0.2983396d, null, 10)")
    assert(df.first().isNullAt(0))
  }

  it should "return null for null resolution" in {
    val df = sparkSession.sql(s"SELECT $functionName(35.8466667d, -0.2983396d, null)")
    assert(df.first().isNullAt(0))
  }

  it should "support compiled function" in {
    import sparkSession.implicits._
    val df = Seq((35.8466667d, -0.2983396d)).toDF("lat", "lng")
    val resolution = 10
    val result = df.select(h3_from_geo(column("lat"), column("lng"), resolution).alias("h3"))
    val h3 = result.first().getAs[Long](0)
    assert(h3 === 0x8A382ED85C37FFFL)
  }

  it should "return null for invalid resolution" in {
    invalidResolutions.foreach { resolution =>
      val df = sparkSession.sql(s"SELECT $functionName(35.8466667d, -0.2983396d, $resolution)")
      assert(df.first().isNullAt(0))
    }
  }

  it should "fail for invalid parameters when ansi enabled" in {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      assertThrows[IllegalArgumentException] {
        sparkSession.sql(s"SELECT $functionName(35.8466667d, -0.2983396d, -1)").collect()
      }
    }
  }

  protected override def functionName: String = "h3_from_geo"
}
