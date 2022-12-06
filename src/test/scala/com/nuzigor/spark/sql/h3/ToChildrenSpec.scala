/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.spark.sql.h3.functions._
import org.apache.spark.sql.functions.column
import org.apache.spark.sql.internal.SQLConf

class ToChildrenSpec extends H3Spec {
  it should "return children of h3 index" in {
    val h3 = 622485130170302463L
    val df = sparkSession.sql(s"SELECT $functionName(${h3}l, 11)")
    val children = df.first().getAs[Seq[Long]](0)
    assert(children.size === 7)
    val childH3 = 626988729797656575L
    assert(children.contains(childH3))
  }

  it should "return null for null h3" in {
    val df = sparkSession.sql(s"SELECT $functionName(null, 11)")
    assert(df.first().isNullAt(0))
  }

  it should "return null for null resolution" in {
    val h3 = 622485130170302463L
    val df = sparkSession.sql(s"SELECT $functionName(${h3}l, null)")
    assert(df.first().isNullAt(0))
  }

  it should "return null for invalid h3" in {
    val df = sparkSession.sql(s"SELECT $functionName(0l, 2)")
    assert(df.first().isNullAt(0))
  }

  it should "support compiled function" in {
    import sparkSession.implicits._
    val h3 = 622485130170302463L
    val df = Seq((h3, 1)).toDF("h3", "id")
    val resolution = 11
    val result = df.select(h3_to_children(column("h3"), resolution).alias("children"))
    val children = result.first().getAs[Seq[Long]](0)
    assert(children.size === 7)
    val childH3 = 626988729797656575L
    assert(children.contains(childH3))
  }

  it should "return null for higher child resolution" in {
    val h3 = 622485130170302463L
    val df = sparkSession.sql(s"SELECT $functionName(${h3}l, 6)")
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
    val h3 = 622485130170302463L
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      Seq(
        s"SELECT $functionName(${h3}l, -1)",
        s"SELECT $functionName(${h3}l, -6)"
      ).foreach { script =>
        assertThrows[IllegalArgumentException] {
          sparkSession.sql(script).collect()
        }
      }
    }
  }

  protected override def functionName: String = "h3_to_children"
}
