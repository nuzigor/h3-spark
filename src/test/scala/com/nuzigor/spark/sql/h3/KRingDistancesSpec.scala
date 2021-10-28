/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.spark.sql.h3.functions._
import org.apache.spark.sql.functions.col

class KRingDistancesSpec extends H3Spec {
  it should "create rings around h3 index" in {
    val h3 = 622485130170302463L
    val df = sparkSession.sql(s"SELECT $functionName(${h3}l, 1)")
    val ring = df.first().getAs[Seq[Seq[Long]]](0)
    assert(ring.size === 2)
    assert(ring.head.head === h3)
    assert(ring(1).size === 6)
  }

  it should "return null for null h3" in {
    val df = sparkSession.sql(s"SELECT $functionName(null, 2)")
    assert(df.first().isNullAt(0))
  }

  it should "return null for null resolution" in {
    val h3 = 622485130170302463L
    val df = sparkSession.sql(s"SELECT $functionName(${h3}l, null)")
    assert(df.first().isNullAt(0))
  }

  it should "not return null for invalid h3" in {
    val df = sparkSession.sql(s"SELECT $functionName(0l, 2)")
    assert(!df.first().isNullAt(0))
  }

  it should "support compiled function" in {
    import sparkSession.implicits._
    val h3 = 622485130170302463L
    val df = Seq((h3, 1)).toDF("h3", "id")
    val result = df.select(h3_k_ring_distances(col("h3"), 1))
    val ring = result.first().getAs[Seq[Seq[Long]]](0)
    assert(ring.size === 2)
    assert(ring.head.head === h3)
    assert(ring(1).size === 6)
  }

  protected override def functionName: String = "h3_k_ring_distances"
}
