/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.spark.sql.h3.functions._
import org.apache.spark.sql.functions.column

class KRingSpec extends H3Spec {
  it should "create ring around h3 index" in {
    val h3 = 622485130170302463L
    val spatialDf = sparkSession.sql(s"SELECT h3_k_ring(${h3}l, 1)")
    val ring = spatialDf.first().getAs[Seq[Long]](0)
    assert(ring.size === 7)
    assert(ring.contains(h3))
  }

  it should "return null for null h3" in {
    val spatialDf = sparkSession.sql(s"SELECT h3_k_ring(null, 2)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "return null for null resolution" in {
    val h3 = 622485130170302463L
    val spatialDf = sparkSession.sql(s"SELECT h3_k_ring(${h3}l, null)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "not return null for invalid h3" in {
    val spatialDf = sparkSession.sql(s"SELECT h3_k_ring(0l, 2)")
    assert(!spatialDf.first().isNullAt(0))
  }

  it should "support compiled function" in {
    import sparkSession.implicits._
    val h3 = 622485130170302463L
    val df = Seq((h3, 1)).toDF("h3", "id")
    val result = df.select(h3_k_ring(column("h3"), 1).alias("ring"))
    val ring = result.first().getAs[Seq[Long]](0)
    assert(ring.size === 7)
    assert(ring.contains(h3))
  }

  protected override def functionName: String = "h3_k_ring"
}
