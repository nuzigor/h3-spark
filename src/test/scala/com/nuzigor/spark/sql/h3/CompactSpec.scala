/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.spark.sql.h3.functions._
import com.uber.h3core.exceptions.H3Exception
import org.apache.spark.sql.functions.column
import org.apache.spark.sql.internal.SQLConf

class CompactSpec extends H3Spec {
  it should "compact h3 indices" in {
    val h3 = 622485130170302463L
    val df = sparkSession.sql(s"SELECT $functionName(h3_k_ring(${h3}l, 3))")
    val compacted = df.first().getAs[Seq[Long]](0)
    assert(compacted.size < 30)
  }

  it should "return null for null h3" in {
    val df = sparkSession.sql(s"SELECT $functionName(null)")
    assert(df.first().isNullAt(0))
  }

  it should "return null for null array elements" in {
    val h3 = 622485130170302463L
    val df = sparkSession.sql(s"SELECT $functionName(array(${h3}l, null))")
    assert(df.first().isNullAt(0))
  }

  it should "not return null for invalid h3" in {
    val df = sparkSession.sql(s"SELECT $functionName(array(0, -1))")
    assert(!df.first().isNullAt(0))
  }

  it should "support compiled function" in {
    import sparkSession.implicits._
    val df = Seq((622485130170302463L, 1)).toDF("h3", "id")
    val resolution = 3
    val result = df.select(h3_compact(h3_k_ring(column("h3"), resolution)).alias("h3"))
    val compacted = result.first().getAs[Seq[Long]](0)
    assert(compacted.size < 30)
  }

  it should "return null for duplicate h3 indices" in {
    val h3 = 622485130170302463L
    val ringCall = s"h3_k_ring(${h3}l, 5)"
    val df = sparkSession.sql(s"SELECT $functionName(concat($ringCall, $ringCall))")
    assert(df.first().isNullAt(0))
  }

  it should "fail for invalid parameters when ansi enabled" in {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      assertThrows[H3Exception] {
        val h3 = 622485130170302463L
        val ringCall = s"h3_k_ring(${h3}l, 5)"
        sparkSession.sql(s"SELECT $functionName(concat($ringCall, $ringCall))").collect()
      }
    }
  }

  protected override def functionName: String = "h3_compact"
}
