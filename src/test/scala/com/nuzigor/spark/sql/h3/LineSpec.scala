/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.spark.sql.h3.functions._
import org.apache.spark.sql.functions.column

class LineSpec extends H3Spec {
  it should "return indices line between start and end indices" in {
    val start = 622485130170957823L
    val end = 622485130170302463L
    val spatialDf = sparkSession.sql(s"SELECT h3_line(${start}l, ${end}l)")
    val line = spatialDf.first().getAs[Seq[Long]](0)
    assert(line.size === 5)
    assert(line.contains(start))
    assert(line.contains(end))
  }

  it should "return null for null start" in {
    val end = 622485130170302463L
    val spatialDf = sparkSession.sql(s"SELECT h3_line(null, ${end}l)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "return null for null end" in {
    val start = 622485130170957823L
    val spatialDf = sparkSession.sql(s"SELECT h3_line(${start}l, null)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "return null for invalid start" in {
    val end = 622485130170302463L
    val spatialDf = sparkSession.sql(s"SELECT h3_line(-1l, ${end}l)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "return null for invalid end" in {
    val start = 622485130170957823L
    val spatialDf = sparkSession.sql(s"SELECT h3_line(${start}l, -1l)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "support compiled function" in {
    import sparkSession.implicits._
    val start = 622485130170957823L
    val end = 622485130170302463L
    val df = Seq((start, end)).toDF("start", "end")
    val result = df.select(h3_line(column("start"), column("end")).alias("line"))
    val line = result.first().getAs[Seq[Long]](0)
    assert(line.size === 5)
    assert(line.contains(start))
    assert(line.contains(end))
  }

  it should "return null for indices around pentagon" in {
    val start = 612630286896726015L
    val end = 612630286919794687L
    val spatialDf = sparkSession.sql(s"SELECT h3_line(${start}l, ${end}l)")
    assert(spatialDf.first().isNullAt(0))
  }

  protected override def functionName: String = "h3_line"
}
