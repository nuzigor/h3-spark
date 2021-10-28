/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.h3.H3
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.types.{BooleanType, DataType, LongType}

/**
 * Returns whether or not the provided h3 indexes are neighbors.
 *
 * @param originExpr h3 origin.
 * @param destinationExpr h3 destination.
 */
@ExpressionDescription(
  usage = "_FUNC_(origin, destination) - Returns true if the provided h3 indices are neighbors.",
  arguments = """
       Arguments:
         * origin - origin h3 index
             622485130170302463l
         * destination - destination h3 index
             622485130170957823l
     """,
  examples = """
       Examples:
         > SELECT _FUNC_(622485130170302463l, 622485130170957823l);
          false
     """,
  since = "0.7.0")
case class AreNeighbors(originExpr: Expression, destinationExpr: Expression)
  extends BinaryExpression with CodegenFallback with ImplicitCastInputTypes with NullIntolerant {

  override def left: Expression = originExpr
  override def right: Expression = destinationExpr
  override def inputTypes: Seq[DataType] = Seq(LongType, LongType)
  override def dataType: DataType = BooleanType

  override protected def nullSafeEval(startAny: Any, endAny: Any): Any = {
    val origin = startAny.asInstanceOf[Long]
    val destination = endAny.asInstanceOf[Long]
    H3.getInstance().h3IndexesAreNeighbors(origin, destination)
  }
}
