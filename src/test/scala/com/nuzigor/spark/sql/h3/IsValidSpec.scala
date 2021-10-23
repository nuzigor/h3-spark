/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.spark.sql.h3.functions._
import org.apache.spark.sql.functions.column

class IsValidSpec extends H3Spec {
  it should "return true for valid h3 index" in {
    val h3 = 622485130170302463L
    val spatialDf = sparkSession.sql(s"SELECT h3_is_valid(${h3}l)")
    val isValid = spatialDf.first().getAs[Boolean](0)
    assert(isValid)
  }

  it should "return false for invalid h3 index" in {
    val spatialDf = sparkSession.sql("SELECT h3_is_valid(-1l)")
    val isValid = spatialDf.first().getAs[Boolean](0)
    assert(!isValid)
  }

  it should "return null for null h3 index" in {
    val spatialDf = sparkSession.sql("SELECT h3_is_valid(null)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "support compiled function" in {
    import sparkSession.implicits._
    val h3 = 622485130170302463L
    val df = Seq((h3, 1)).toDF("h3", "id")
    val result = df.select(h3_is_valid(column("h3")).alias("valid"))
    val isValid = result.first().getAs[Boolean](0)
    assert(isValid)
  }

  protected override def functionName: String = "h3_is_valid"
}
