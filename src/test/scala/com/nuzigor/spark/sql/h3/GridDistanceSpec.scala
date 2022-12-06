/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.spark.sql.h3.functions._
import com.uber.h3core.exceptions.H3Exception
import org.apache.spark.sql.functions.column
import org.apache.spark.sql.internal.SQLConf

class GridDistanceSpec extends H3Spec {
  it should "return distance in hexes between start and end indices" in {
    val start = 622485130170957823L
    val end = 622485130170302463L
    val df = sparkSession.sql(s"SELECT $functionName(${start}l, ${end}l)")
    val distance = df.first().getAs[Long](0)
    assert(distance === 4)
  }

  it should "return null for invalid start" in {
    val end = 622485130170302463L
    val df = sparkSession.sql(s"SELECT $functionName(-1, ${end}l)")
    assert(df.first().isNullAt(0))
  }

  it should "return null for invalid end" in {
    val start = 622485130170957823L
    val df = sparkSession.sql(s"SELECT $functionName(${start}l, -1)")
    assert(df.first().isNullAt(0))
  }

  it should "return null for null start" in {
    val end = 622485130170302463L
    val df = sparkSession.sql(s"SELECT $functionName(null, ${end}l)")
    assert(df.first().isNullAt(0))
  }

  it should "return null for null end" in {
    val start = 622485130170957823L
    val df = sparkSession.sql(s"SELECT $functionName(${start}l, null)")
    assert(df.first().isNullAt(0))
  }

  it should "support compiled function" in {
    import sparkSession.implicits._
    val df = Seq((622485130170957823L, 622485130170302463L)).toDF("start", "end")
    val result = df.select(h3_grid_distance(column("start"), column("end")).alias("h3"))
    val distance = result.first().getAs[Long](0)
    assert(distance === 4)
  }

  it should "return null for indices around pentagon" in {
    val start = 612630286896726015L
    val end = 612630286919794687L
    val df = sparkSession.sql(s"SELECT $functionName(${start}l, ${end}l)")
    assert(df.first().isNullAt(0))
  }

  it should "fail for invalid parameters when ansi enabled" in {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      assertThrows[H3Exception] {
        val start = 612630286896726015L
        val end = 612630286919794687L
        sparkSession.sql(s"SELECT $functionName(${start}l, ${end}l)").collect()
      }
    }
  }

  protected override def functionName: String = "h3_grid_distance"
}
