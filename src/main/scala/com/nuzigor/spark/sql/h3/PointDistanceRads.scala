/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.uber.h3core.LengthUnit
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}

/**
 * Gives the "great circle" or "haversine" distance between centers of h3 indices in radians.
 *
 * @param startExpr h3 start.
 * @param endExpr h3 end.
 */
@ExpressionDescription(
  usage = "_FUNC_(start, end) - Gives the \"great circle\" or \"haversine\" distance between centers of h3 indices in radians.",
  arguments = """
       Arguments:
         * start - start h3 index
             622485130170302463l
         * end - end h3 index
             622485130170957823l
     """,
  examples = """
       Examples:
         > SELECT _FUNC_(622485130170302463l, 622485130170957823l);
          7.471E-5
     """,
  since = "0.7.0")
case class PointDistanceRads(startExpr: Expression, endExpr: Expression) extends PointDistance {
  override def unit: LengthUnit = LengthUnit.rads
}
