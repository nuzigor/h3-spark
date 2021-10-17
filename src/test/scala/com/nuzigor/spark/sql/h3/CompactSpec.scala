/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

class CompactSpec extends H3Spec {
  it should "compact h3 indices" in {
    val h3 = 622485130170302463L
    val spatialDf = sparkSession.sql(s"SELECT h3_compact(h3_k_ring(${h3}l, 3))")
    val compacted = spatialDf.first().getAs[Seq[Long]](0)
    assert(compacted.size < 30)
  }

  it should "return null for null h3" in {
    val spatialDf = sparkSession.sql(s"SELECT h3_compact(null)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "return null for null array elements" in {
    val h3 = 622485130170302463L
    val spatialDf = sparkSession.sql(s"SELECT h3_compact(array(${h3}l, null))")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "not return null for invalid h3" in {
    val spatialDf = sparkSession.sql(s"SELECT h3_compact(array(0, -1))")
    assert(!spatialDf.first().isNullAt(0))
  }
}
