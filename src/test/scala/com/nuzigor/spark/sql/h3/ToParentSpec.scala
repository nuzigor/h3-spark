/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.spark.sql.h3.functions._
import org.apache.spark.sql.functions.column

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

  it should "support compiled function" in {
    import sparkSession.implicits._
    val h3 = 622485130170302463L
    val df = Seq((h3, 1)).toDF("h3", "id")
    val resolution = 8
    val result = df.select(h3_to_parent(column("h3"), resolution).alias("parent"))
    val parent = result.first().getAs[Long](0)
    assert(parent === 613477930917429247L)
  }

  it should "return null for lower parent resolution" in {
    val h3 = 622485130170302463L
    val spatialDf = sparkSession.sql(s"SELECT h3_to_parent(${h3}l, 12)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "return null for invalid resolution" in {
    val h3 = 622485130170302463L
    invalidResolutions.foreach { resolution =>
      val spatialDf = sparkSession.sql(s"SELECT h3_to_parent(${h3}l, $resolution)")
      assert(spatialDf.first().isNullAt(0))
    }
  }

  protected override def functionName: String = "h3_to_parent"
}
