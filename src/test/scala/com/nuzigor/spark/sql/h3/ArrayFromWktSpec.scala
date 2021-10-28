/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.spark.sql.h3.functions._
import org.apache.spark.sql.functions.column
import org.apache.spark.sql.internal.SQLConf

class ArrayFromWktSpec extends H3Spec {
  it should "convert WKT polygon to h3" in {
    val df = sparkSession.sql(s"SELECT $functionName('POLYGON ((3 -1, 3 -1.1, 3.1 1.1, 3 -1))', 7)")
    val h3 = df.first().getAs[Seq[Long]](0)
    assert(h3.nonEmpty)
  }

  it should "convert WKT polygon with holes to h3" in {
    val polygon1 = "POLYGON((0.5 0.5,5 0,5 5,0 5,0.5 0.5), (1.5 1,4 3,4 1,1.5 1))"
    val polygon2 = "POLYGON((0.5 0.5,5 0,5 5,0 5,0.5 0.5))"
    val df = sparkSession.sql(s"SELECT $functionName('$polygon1', 7), h3_array_from_wkt('$polygon2', 7)")
    val h3Holes = df.first().getAs[Seq[Long]](0)
    val h3Solid = df.first().getAs[Seq[Long]](1)
    assert(h3Holes.size < h3Solid.size)
  }

  it should "return single element for point WKT" in {
    val df = sparkSession.sql(s"SELECT $functionName('POINT (-0.2983396 35.8466667)', 7)")
    val h3 = df.first().getAs[Seq[Long]](0)
    assert(h3.size === 1)
  }

  it should "return element for each point of multi point WKT" in {
    val df = sparkSession.sql(s"SELECT $functionName('MULTIPOINT ((-0.2983396 35.8466667), (35.8466667 -0.2983396))', 7)")
    val h3 = df.first().getAs[Seq[Long]](0)
    assert(h3.size === 2)
  }

  it should "merge close points of multi point WKT" in {
    val df = sparkSession.sql(s"SELECT $functionName('MULTIPOINT ((-0.2983396 35.8466667), (-0.29833 35.84666))', 7)")
    val h3 = df.first().getAs[Seq[Long]](0)
    assert(h3.size === 1)
  }

  it should "return h3 indices for line segments of line string WKT" in {
    val df = sparkSession.sql(s"SELECT $functionName('LINESTRING (-0.29 35.75, -0.30 35.83, -0.31 35.96)', 7)")
    val h3 = df.first().getAs[Seq[Long]](0)
    assert(h3.size === 12)
    assert(h3.size === h3.distinct.size)
  }

  it should "return h3 indices for all line segments of multi line string WKT" in {
    val multiLineString = "MULTILINESTRING ((-0.29 35.75, -0.30 35.83, -0.31 35.96), (-1.29 38.75, -1.30 38.83, -1.31 38.96))"
    val df = sparkSession.sql(s"SELECT $functionName('$multiLineString', 7)")
    val h3 = df.first().getAs[Seq[Long]](0)
    assert(h3.size === 24)
    assert(h3.size === h3.distinct.size)
  }

  it should "merge h3 indices for close line segments of multi line string WKT" in {
    val multiLineString = "MULTILINESTRING ((-0.29 35.75, -0.30 35.83, -0.31 35.96), (-0.27 35.78, -0.30 35.83, -0.31 35.95))"
    val df = sparkSession.sql(s"SELECT $functionName('$multiLineString', 7)")
    val h3 = df.first().getAs[Seq[Long]](0)
    assert(h3.size === 14)
    assert(h3.size === h3.distinct.size)
  }

  it should "convert WKT multi polygon to h3" in {
    val df = sparkSession.sql(s"SELECT $functionName('MULTIPOLYGON (((3 -1, 3 -1.1, 3.1 1.1, 3 -1)), ((6 -1, 6 -1.1, 6.1 1.1, 6 -1)))', 7)")
    val h3 = df.first().getAs[Seq[Long]](0)
    assert(h3.nonEmpty)
    assert(h3.size === h3.distinct.size)
  }

  it should "return null for empty WKT polygon" in {
    val df = sparkSession.sql(s"SELECT $functionName('POLYGON EMPTY', 10)")
    assert(df.first().isNullAt(0))
  }

  it should "return null for invalid WKT" in {
    val df = sparkSession.sql(s"SELECT $functionName('bla bla', 10)")
    assert(df.first().isNullAt(0))
  }

  it should "return null for null WKT" in {
    val df = sparkSession.sql(s"SELECT $functionName(null, 10)")
    assert(df.first().isNullAt(0))
  }

  it should "return null for null resolution" in {
    val df = sparkSession.sql(s"SELECT $functionName('POLYGON ((3 -1, 3 -1.1, 3.1 1.1, 3 -1))', null)")
    assert(df.first().isNullAt(0))
  }

  it should "support compiled function" in {
    import sparkSession.implicits._
    val df = Seq(("POLYGON ((3 -1, 3 -1.1, 3.1 1.1, 3 -1))", 1)).toDF("wkt", "id")
    val resolution = 7
    val result = df.select(h3_array_from_wkt(column("wkt"), resolution).alias("h3"))
    val h3 = result.first().getAs[Seq[Long]](0)
    assert(h3.nonEmpty)
  }

  it should "return null for invalid resolution" in {
    invalidResolutions.foreach { resolution =>
      val df = sparkSession.sql(s"SELECT $functionName('POLYGON ((3 -1, 3 -1.1, 3.1 1.1, 3 -1))', $resolution)")
      assert(df.first().isNullAt(0))
    }
  }

  it should "fail for invalid parameters when ansi enabled" in {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      Seq(
        s"SELECT $functionName('bla bla', 10)",
        s"SELECT $functionName('POLYGON ((3 -1, 3 -1.1, 3.1 1.1, 3 -1))', -1)",
        s"SELECT $functionName('POLYGON ((3 -1, 3 -1.1, 3.1 1.1, 3 -1))', 16)",
      ).foreach { script =>
        assertThrows[Throwable] {
          sparkSession.sql(script).collect()
        }
      }
    }
  }

  protected override def functionName: String = "h3_array_from_wkt"
}
