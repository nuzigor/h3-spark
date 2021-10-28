/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.spark.sql.h3.functions._
import org.apache.spark.sql.functions.col
import org.scalactic.Tolerance._

class PointDistanceKmSpec extends H3Spec {
  it should "return distance in km between start and end indices" in {
    val start = 622485130170957823L
    val end = 622485130170302463L
    val spatialDf = sparkSession.sql(s"SELECT $functionName(${start}l, ${end}l)")
    val distance = spatialDf.first().getAs[Double](0)
    assert(distance === 0.475 +- 0.001)
  }

  it should "return null for null start" in {
    val end = 622485130170302463L
    val spatialDf = sparkSession.sql(s"SELECT $functionName(null, ${end}l)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "return null for null end" in {
    val start = 622485130170957823L
    val spatialDf = sparkSession.sql(s"SELECT $functionName(${start}l, null)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "support compiled function" in {
    import sparkSession.implicits._
    val df = Seq((622485130170957823L, 622485130170302463L)).toDF("start", "end")
    val result = df.select(h3_distance_km(col("start"), col("end")))
    val distance = result.first().getAs[Double](0)
    assert(distance === 0.475 +- 0.001)
  }

  protected override def functionName: String = "h3_distance_km"
}
