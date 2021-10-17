package com.nuzigor.spark.sql.h3

class FromWktSpec extends H3Spec {
  it should "convert WKT point to h3" in {
    val spatialDf = sparkSession.sql("SELECT h3_from_wkt('POINT (-0.2983396 35.8466667)', 10)")
    val h3 = spatialDf.first().getAs[Long](0)
    assert(h3 === 0x8A382ED85C37FFFL)
  }

  it should "return null for empty WKT point" in {
    val spatialDf = sparkSession.sql("SELECT h3_from_wkt('POINT EMPTY', 10)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "return null for invalid WKT" in {
    val spatialDf = sparkSession.sql("SELECT h3_from_wkt('bla bla', 10)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "return null for null WKT" in {
    val spatialDf = sparkSession.sql("SELECT h3_from_wkt(null, 10)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "return null for null resolution" in {
    val spatialDf = sparkSession.sql("SELECT h3_from_wkt('POINT (-0.2983396 35.8466667)', null)")
    assert(spatialDf.first().isNullAt(0))
  }
}
