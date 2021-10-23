/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.spark.sql.h3.functions._
import org.apache.spark.sql.functions.column

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

  it should "support compiled function" in {
    import sparkSession.implicits._
    val h3 = 622485130170302463L
    val df = Seq((h3, 1)).toDF("h3", "id")
    val resolution = 11
    val result = df.select(h3_to_center_child(column("h3"), resolution).alias("child"))
    val child = result.first().getAs[Long](0)
    assert(child === 626988729797644287L)
  }

  protected override def functionName: String = "h3_to_center_child"
}
