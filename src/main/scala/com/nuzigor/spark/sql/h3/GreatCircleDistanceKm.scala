/*
 * Copyright 2021 Igor Nuzhnov
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.uber.h3core.LengthUnit
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}

/**
 * Gives the "great circle" or "haversine" distance between centers of h3 indices in kilometers.
 *
 * @param left h3 start.
 * @param right h3 end.
 */
@ExpressionDescription(
  usage = "_FUNC_(start, end) - Gives the \"great circle\" or \"haversine\" distance between centers of h3 indices in kilometers.",
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
          0.475
     """,
  since = "0.7.0"
)
case class GreatCircleDistanceKm(left: Expression, right: Expression) extends GreatCircleDistance {
  override def unit: LengthUnit = LengthUnit.km

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): GreatCircleDistanceKm =
    copy(left = newLeft, right = newRight)
}
