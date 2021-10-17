package com.nuzigor.spark.sql.h3

class ArrayFromWktSpec extends H3Spec {
  it should "convert WKT polygon to h3" in {
    val spatialDf = sparkSession.sql("SELECT h3_array_from_wkt('POLYGON ((3 -1, 3 -1.1, 3.1 1.1, 3 -1))', 7)")
    val h3 = spatialDf.first().getAs[Seq[Long]](0)
    assert(h3.nonEmpty)
  }

  it should "convert WKT polygon with holes to h3" in {
    val spatialDf = sparkSession.sql("SELECT h3_array_from_wkt('POLYGON((0.5 0.5,5 0,5 5,0 5,0.5 0.5), (1.5 1,4 3,4 1,1.5 1))', 7), h3_array_from_wkt('POLYGON((0.5 0.5,5 0,5 5,0 5,0.5 0.5))', 7)")
    val h3Holes = spatialDf.first().getAs[Seq[Long]](0)
    val h3Solid = spatialDf.first().getAs[Seq[Long]](1)
    assert(h3Holes.size < h3Solid.size)
  }

  it should "return single element for point WKT" in {
    val spatialDf = sparkSession.sql("SELECT h3_array_from_wkt('POINT (-0.2983396 35.8466667)', 7)")
    val h3 = spatialDf.first().getAs[Seq[Long]](0)
    assert(h3.size === 1)
  }

  it should "return element for each point of multi point WKT" in {
    val spatialDf = sparkSession.sql("SELECT h3_array_from_wkt('MULTIPOINT ((-0.2983396 35.8466667), (35.8466667 -0.2983396))', 7)")
    val h3 = spatialDf.first().getAs[Seq[Long]](0)
    assert(h3.size === 2)
  }

  it should "merge close points of multi point WKT" in {
    val spatialDf = sparkSession.sql("SELECT h3_array_from_wkt('MULTIPOINT ((-0.2983396 35.8466667), (-0.29833 35.84666))', 7)")
    val h3 = spatialDf.first().getAs[Seq[Long]](0)
    assert(h3.size === 1)
  }

  it should "return h3 indices for line segments of line string WKT" in {
    val spatialDf = sparkSession.sql("SELECT h3_array_from_wkt('LINESTRING (-0.29 35.75, -0.30 35.83, -0.31 35.96)', 7)")
    val h3 = spatialDf.first().getAs[Seq[Long]](0)
    assert(h3.size === 12)
    assert(h3.size === h3.distinct.size)
  }

  it should "return h3 indices for all line segments of multi line string WKT" in {
    val spatialDf = sparkSession.sql("SELECT h3_array_from_wkt('MULTILINESTRING ((-0.29 35.75, -0.30 35.83, -0.31 35.96), (-1.29 38.75, -1.30 38.83, -1.31 38.96))', 7)")
    val h3 = spatialDf.first().getAs[Seq[Long]](0)
    assert(h3.size === 24)
    assert(h3.size === h3.distinct.size)
  }

  it should "merge h3 indices for close line segments of multi line string WKT" in {
    val spatialDf = sparkSession.sql("SELECT h3_array_from_wkt('MULTILINESTRING ((-0.29 35.75, -0.30 35.83, -0.31 35.96), (-0.27 35.78, -0.30 35.83, -0.31 35.95))', 7)")
    val h3 = spatialDf.first().getAs[Seq[Long]](0)
    assert(h3.size === 14)
    assert(h3.size === h3.distinct.size)
  }

  it should "convert WKT multi polygon to h3" in {
    val spatialDf = sparkSession.sql("SELECT h3_array_from_wkt('MULTIPOLYGON (((3 -1, 3 -1.1, 3.1 1.1, 3 -1)), ((6 -1, 6 -1.1, 6.1 1.1, 6 -1)))', 7)")
    val h3 = spatialDf.first().getAs[Seq[Long]](0)
    assert(h3.nonEmpty)
    assert(h3.size === h3.distinct.size)
  }

  it should "return null for empty WKT polygon" in {
    val spatialDf = sparkSession.sql("SELECT h3_array_from_wkt('POLYGON EMPTY', 10)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "return null for invalid WKT" in {
    val spatialDf = sparkSession.sql("SELECT h3_array_from_wkt('bla bla', 10)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "return null for null WKT" in {
    val spatialDf = sparkSession.sql("SELECT h3_array_from_wkt(null, 10)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "return null for null resolution" in {
    val spatialDf = sparkSession.sql("SELECT h3_array_from_wkt('POLYGON ((3 -1, 3 -1.1, 3.1 1.1, 3 -1))', null)")
    assert(spatialDf.first().isNullAt(0))
  }
}
