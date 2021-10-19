/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

class ToCenterChildSpec extends H3Spec {
  it should "return center child of h3 index" in {
    val h3 = 622485130170302463L
    val spatialDf = sparkSession.sql(s"SELECT h3_to_center_child(${h3}l, 11)")
    val child = spatialDf.first().getAs[Long](0)
    assert(child === 626988729797644287L)
  }

  it should "return null for null h3" in {
    val spatialDf = sparkSession.sql(s"SELECT h3_to_center_child(null, 11)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "return null for null resolution" in {
    val h3 = 622485130170302463L
    val spatialDf = sparkSession.sql(s"SELECT h3_to_center_child(${h3}l, null)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "not return null for invalid h3" in {
    val spatialDf = sparkSession.sql(s"SELECT h3_to_center_child(0l, 2)")
    assert(!spatialDf.first().isNullAt(0))
  }

  protected override def functionName: String = "h3_to_center_child"
}
