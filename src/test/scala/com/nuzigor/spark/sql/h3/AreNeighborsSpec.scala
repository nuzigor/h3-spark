/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.spark.sql.h3.functions._
import com.uber.h3core.exceptions.H3Exception
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf

class AreNeighborsSpec extends H3Spec {
  it should "return true if indices are neighbors" in {
    val start = 622485130171842559L
    val end = 622485130170302463L
    val df = sparkSession.sql(s"SELECT $functionName(${start}l, ${end}l)")
    val result = df.first().getAs[Boolean](0)
    assert(result)
  }

  it should "return false if indices are not neighbors" in {
    val start = 622485130170957823L
    val end = 622485130170302463L
    val df = sparkSession.sql(s"SELECT $functionName(${start}l, ${end}l)")
    val result = df.first().getAs[Boolean](0)
    assert(!result)
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
    val df = Seq((622485130171842559L, 622485130170302463L)).toDF("start", "end")
    val result = df.select(h3_are_neighbors(col("start"), col("end")))
    val neighbors = result.first().getAs[Boolean](0)
    assert(neighbors)
  }

  it should "fail for invalid parameters when ansi enabled" in {
    val h3 = 622485130170302463L
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      Seq(s"SELECT $functionName(${h3}l, -1)", s"SELECT $functionName(-1, ${h3}l)").foreach { script =>
        assertThrows[H3Exception] {
          sparkSession.sql(script).collect()
        }
      }
    }
  }

  protected override def functionName: String = "h3_are_neighbors"
}
