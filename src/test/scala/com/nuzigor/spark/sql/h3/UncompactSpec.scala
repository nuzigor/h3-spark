/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

class UncompactSpec extends H3Spec {
  it should "uncompact h3 indices" in {
    val h3 = 622485130170302463L
    val spatialDf = sparkSession.sql(s"SELECT h3_uncompact(h3_k_ring(${h3}l, 1), 11)")
    val uncompacted = spatialDf.first().getAs[Seq[Long]](0)
    assert(uncompacted.size > 7)
  }

  it should "return null for null h3" in {
    val spatialDf = sparkSession.sql(s"SELECT h3_uncompact(null, 10)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "return null for null array elements" in {
    val h3 = 622485130170302463L
    val spatialDf = sparkSession.sql(s"SELECT h3_uncompact(array(${h3}l, null), 11)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "return null for null resolution" in {
    val h3 = 622485130170302463L
    val spatialDf = sparkSession.sql(s"SELECT h3_uncompact(array(${h3}l), null)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "not return null for invalid h3" in {
    val spatialDf = sparkSession.sql(s"SELECT h3_uncompact(array(0, 2), 3)")
    assert(!spatialDf.first().isNullAt(0))
  }

  protected override def functionName: String = "h3_uncompact"
}
