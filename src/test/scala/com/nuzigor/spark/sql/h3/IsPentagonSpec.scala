/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.spark.sql.h3.functions._
import org.apache.spark.sql.functions.col

import scala.collection.JavaConverters._

class IsPentagonSpec extends H3Spec {
  it should "return true for pentagon h3 index" in {
    val resolution = 7
    val index = h3.getPentagonIndexes(resolution).asScala.head
    val spatialDf = sparkSession.sql(s"SELECT $functionName(${index}l)")
    val isPentagon = spatialDf.first().getAs[Boolean](0)
    assert(isPentagon)
  }

  it should "return false for normal h3 index" in {
    val h3 = 622485130170302463L
    val spatialDf = sparkSession.sql(s"SELECT $functionName(${h3}l)")
    val isPentagon = spatialDf.first().getAs[Boolean](0)
    assert(!isPentagon)
  }

  it should "return null for null h3 index" in {
    val spatialDf = sparkSession.sql(s"SELECT $functionName(null)")
    assert(spatialDf.first().isNullAt(0))
  }

  it should "support compiled function" in {
    import sparkSession.implicits._
    val resolution = 7
    val index = h3.getPentagonIndexes(resolution).asScala.head
    val df = Seq((index, 1)).toDF("h3", "id")
    val result = df.select(h3_is_pentagon(col("h3")))
    val isPentagon = result.first().getAs[Boolean](0)
    assert(isPentagon)
  }

  protected override def functionName: String = "h3_is_pentagon"
}
