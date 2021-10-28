/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.uber.h3core.AreaUnit
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}

/**
 * Exact area of specific cell in square meters.
 *
 * @param h3Expr h3 index.
 */
@ExpressionDescription(
  usage = "_FUNC_(h3) - Returns the exact area of specific cell in square meters.",
  arguments = """
       Arguments:
         * h3 - h3 index
             622485130170302463l
     """,
  examples = """
       Examples:
         > SELECT _FUNC_(622485130170957823l);
          14812.0
     """,
  since = "0.7.0")
case class CellAreaM2(h3Expr: Expression) extends CellArea {
  override def unit: AreaUnit = AreaUnit.m2
}
