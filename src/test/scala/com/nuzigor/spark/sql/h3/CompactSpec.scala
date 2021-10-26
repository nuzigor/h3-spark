/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.spark.sql.h3.functions._
import org.apache.spark.sql.functions.column

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

  it should "support compiled function" in {
    import sparkSession.implicits._
    val df = Seq((622485130170302463L, 1)).toDF("h3", "id")
    val resolution = 3
    val result = df.select(h3_compact(h3_k_ring(column("h3"), resolution)).alias("h3"))
    val compacted = result.first().getAs[Seq[Long]](0)
    assert(compacted.size < 30)
  }

  it should "return null for duplicate h3 indices" in {
    val h3 = 622485130170302463L
    val ringCall = s"h3_k_ring(${h3}l, 5)"
    val spatialDf = sparkSession.sql(s"SELECT h3_compact(concat($ringCall, $ringCall))")
    assert(spatialDf.first().isNullAt(0))
  }

  protected override def functionName: String = "h3_compact"
}
