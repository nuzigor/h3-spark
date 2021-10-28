/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.spark.sql.h3.functions._
import org.apache.spark.sql.functions.col
import org.scalactic.Tolerance._

class CellAreaKm2Spec extends H3Spec {
  it should "return cell area in km2" in {
    val index = 622485130170957823L
    val spatialDf = sparkSession.sql(s"SELECT $functionName(${index}l)")
    val area = spatialDf.first().getAs[Double](0)
    assert(area === 0.0148 +- 0.001)
  }

  it should "return null for null index" in {
    val spatialDf = sparkSession.sql(s"SELECT $functionName(null)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "support compiled function" in {
    import sparkSession.implicits._
    val df = Seq((622485130170957823L, 1)).toDF("h3", "id")
    val result = df.select(h3_cell_area_km2(col("h3")))
    val area = result.first().getAs[Double](0)
    assert(area === 0.0148 +- 0.001)
  }

  protected override def functionName: String = "h3_cell_area_km2"
}
