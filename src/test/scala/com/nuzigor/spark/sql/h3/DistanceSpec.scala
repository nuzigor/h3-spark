/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.spark.sql.h3.functions._
import org.apache.spark.sql.functions.column

class DistanceSpec extends H3Spec {
  it should "return distance in hexes between start and end indices" in {
    val start = 622485130170957823L
    val end = 622485130170302463L
    val spatialDf = sparkSession.sql(s"SELECT h3_distance(${start}l, ${end}l)")
    val distance = spatialDf.first().getAs[Int](0)
    assert(distance === 4)
  }

  it should "return null for invalid start" in {
    val end = 622485130170302463L
    val spatialDf = sparkSession.sql(s"SELECT h3_distance(-1, ${end}l)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "return null for invalid end" in {
    val start = 622485130170957823L
    val spatialDf = sparkSession.sql(s"SELECT h3_distance(${start}l, -1)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "return null for null start" in {
    val end = 622485130170302463L
    val spatialDf = sparkSession.sql(s"SELECT h3_distance(null, ${end}l)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "return null for null end" in {
    val start = 622485130170957823L
    val spatialDf = sparkSession.sql(s"SELECT h3_distance(${start}l, null)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "support compiled function" in {
    import sparkSession.implicits._
    val df = Seq((622485130170957823L, 622485130170302463L)).toDF("start", "end")
    val result = df.select(h3_distance(column("start"), column("end")).alias("h3"))
    val distance = result.first().getAs[Int](0)
    assert(distance === 4)
  }

  protected override def functionName: String = "h3_distance"
}
