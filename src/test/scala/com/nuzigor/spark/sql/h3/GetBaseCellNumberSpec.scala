/*
 * Copyright 2021 Igor Nuzhnov
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.spark.sql.h3.functions._
import org.apache.spark.sql.functions.col

class GetBaseCellNumberSpec extends H3Spec {
  it should "return index for valid h3 index" in {
    val h3 = 622485130170302463L
    val df = sparkSession.sql(s"SELECT $functionName(${h3}l)")
    val h3_base_cell = df.first().getAs[Integer](0)
    assert(h3_base_cell == 28)
  }

  it should "return null for invalid h3 index" in {
    val df = sparkSession.sql(s"SELECT $functionName(-1l)")
    assert(df.first().isNullAt(0))
  }

  it should "return null for null h3 index" in {
    val df = sparkSession.sql(s"SELECT $functionName(null)")
    assert(df.first().isNullAt(0))
  }

  it should "support compiled function" in {
    import sparkSession.implicits._
    val h3 = 622485130170302463L
    val df = Seq((h3, 1)).toDF("h3", "id")
    val result = df.select(h3_base_cell_number(col("h3")).alias("valid"))
    val h3_base_cell = result.first().getAs[Integer](0)
    assert(h3_base_cell == 28)
  }

  protected override def functionName: String = "h3_base_cell_number"
}
