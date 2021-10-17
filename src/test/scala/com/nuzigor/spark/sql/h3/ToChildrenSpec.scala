package com.nuzigor.spark.sql.h3

class ToChildrenSpec extends H3Spec {
  it should "return children of h3 index" in {
    val h3 = 622485130170302463L
    val spatialDf = sparkSession.sql(s"SELECT h3_to_children(${h3}l, 11)")
    val children = spatialDf.first().getAs[Seq[Long]](0)
    assert(children.size === 7)
    assert(children.contains(626988729797656575L))
  }

  it should "return null for null h3" in {
    val spatialDf = sparkSession.sql(s"SELECT h3_to_children(null, 11)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "return null for null resolution" in {
    val h3 = 622485130170302463L
    val spatialDf = sparkSession.sql(s"SELECT h3_to_children(${h3}l, null)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "not return null for invalid h3" in {
    val spatialDf = sparkSession.sql(s"SELECT h3_to_children(0l, 2)")
    assert(!spatialDf.first().isNullAt(0))
  }
}
