/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.spark.sql.h3.functions._
import org.apache.spark.sql.functions.column
import org.apache.spark.sql.internal.SQLConf

class ToParentSpec extends H3Spec {
  it should "return h3 parent index" in {
    val h3 = 622485130170302463L
    val df = sparkSession.sql(s"SELECT $functionName(${h3}l, 8)")
    val parent = df.first().getAs[Long](0)
    assert(parent === 613477930917429247L)
  }

  it should "return null for null h3" in {
    val df = sparkSession.sql(s"SELECT $functionName(null, 8)")
    assert(df.first().isNullAt(0))
  }

  it should "return null for null resolution" in {
    val h3 = 622485130170302463L
    val df = sparkSession.sql(s"SELECT $functionName(${h3}l, null)")
    assert(df.first().isNullAt(0))
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
    val df = sparkSession.sql(s"SELECT $functionName(${h3}l, 12)")
    assert(df.first().isNullAt(0))
  }

  it should "return null for invalid resolution" in {
    val h3 = 622485130170302463L
    invalidResolutions.foreach { resolution =>
      val df = sparkSession.sql(s"SELECT $functionName(${h3}l, $resolution)")
      assert(df.first().isNullAt(0))
    }
  }

  it should "fail for invalid parameters when ansi enabled" in {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      assertThrows[IllegalArgumentException] {
        val h3 = 622485130170302463L
        sparkSession.sql(s"SELECT $functionName(${h3}l, -1)").collect()
      }
    }
  }

  protected override def functionName: String = "h3_to_parent"
}
