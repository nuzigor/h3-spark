package com.nuzigor.spark.sql.h3

class ToParentSpec extends H3Spec {
  it should "return h3 parent index" in {
    val h3 = 622485130170302463L
    val spatialDf = sparkSession.sql(s"SELECT h3_to_parent(${h3}l, 8)")
    val parent = spatialDf.first().getAs[Long](0)
    assert(parent === 613477930917429247L)
  }

  it should "return null for null h3" in {
    val spatialDf = sparkSession.sql(s"SELECT h3_to_parent(null, 8)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "return null for null resolution" in {
    val h3 = 622485130170302463L
    val spatialDf = sparkSession.sql(s"SELECT h3_to_parent(${h3}l, null)")
    assert(spatialDf.first().isNullAt(0))
  }
}
