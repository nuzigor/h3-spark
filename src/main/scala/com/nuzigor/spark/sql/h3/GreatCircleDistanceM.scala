/*
 * Copyright 2021 Igor Nuzhnov
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.uber.h3core.LengthUnit
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}

/**
 * Gives the "great circle" or "haversine" distance between centers of h3 indices in meters.
 *
 * @param left h3 start.
 * @param right h3 end.
 */
@ExpressionDescription(
  usage = "_FUNC_(start, end) - Gives the \"great circle\" or \"haversine\" distance between centers of h3 indices in meters.",
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
          475.9
     """,
  since = "0.7.0"
)
case class GreatCircleDistanceM(left: Expression, right: Expression) extends GreatCircleDistance {
  override def unit: LengthUnit = LengthUnit.m

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): GreatCircleDistanceM =
    copy(left = newLeft, right = newRight)
}
