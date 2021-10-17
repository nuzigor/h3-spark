package com.nuzigor.spark.sql.h3

class GetResolutionSpec extends H3Spec {
  it should "return h3 index resolution" in {
    val h3 = 622485130170302463L
    val spatialDf = sparkSession.sql(s"SELECT h3_get_resolution(${h3}l)")
    val resolution = spatialDf.first().getAs[Int](0)
    assert(resolution === 10)
  }

  it should "return null for null h3" in {
    val spatialDf = sparkSession.sql(s"SELECT h3_get_resolution(null)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "not return null for invalid h3" in {
    val spatialDf = sparkSession.sql(s"SELECT h3_get_resolution(-1)")
    assert(!spatialDf.first().isNullAt(0))
  }
}
