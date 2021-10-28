/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.spark.sql.h3.functions._
import org.apache.spark.sql.functions.column
import org.apache.spark.sql.internal.SQLConf

class UncompactSpec extends H3Spec {
  it should "uncompact h3 indices" in {
    val h3 = 622485130170302463L
    val df = sparkSession.sql(s"SELECT $functionName(h3_k_ring(${h3}l, 1), 11)")
    val uncompacted = df.first().getAs[Seq[Long]](0)
    assert(uncompacted.size > 7)
  }

  it should "return null for null h3" in {
    val df = sparkSession.sql(s"SELECT $functionName(null, 10)")
    assert(df.first().isNullAt(0))
  }

  it should "return null for null array elements" in {
    val h3 = 622485130170302463L
    val df = sparkSession.sql(s"SELECT $functionName(array(${h3}l, null), 11)")
    assert(df.first().isNullAt(0))
  }

  it should "return null for null resolution" in {
    val h3 = 622485130170302463L
    val df = sparkSession.sql(s"SELECT $functionName(array(${h3}l), null)")
    assert(df.first().isNullAt(0))
  }

  it should "not return null for invalid h3" in {
    val df = sparkSession.sql(s"SELECT $functionName(array(0, 2), 3)")
    assert(!df.first().isNullAt(0))
  }

  it should "support compiled function" in {
    import sparkSession.implicits._
    val h3 = 622485130170302463L
    val df = Seq((h3, 1)).toDF("h3", "id")
    val resolution = 11
    val result = df.select(h3_uncompact(h3_k_ring(column("h3"), 1), resolution).alias("result"))
    val uncompacted = result.first().getAs[Seq[Long]](0)
    assert(uncompacted.size > 7)
  }

  it should "return null for invalid resolution" in {
    val h3 = 622485130170302463L
    invalidResolutions.foreach { resolution =>
      val df = sparkSession.sql(s"SELECT $functionName(h3_k_ring(${h3}l, 1), $resolution)")
      assert(df.first().isNullAt(0))
    }
  }

  it should "fail for invalid parameters when ansi enabled" in {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      assertThrows[IllegalArgumentException] {
        val h3 = 622485130170302463L
        sparkSession.sql(s"SELECT $functionName(h3_k_ring(${h3}l, 1), -1)").collect()
      }
    }
  }

  protected override def functionName: String = "h3_uncompact"
}
