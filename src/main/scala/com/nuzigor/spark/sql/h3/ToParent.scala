/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.h3.H3
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType}

/**
 * Returns the parent (coarser) index containing h3.
 *
 * @param h3Expr h3 index.
 * @param parentResolutionExpr parent resolution.
 */
@ExpressionDescription(
  usage = "_FUNC_(h3, resolution) - Returns the parent (coarser) index containing h3.",
  arguments = """
       Arguments:
         * h3 - child h3 index
             622485130170302463l
         * resolution - parent index resolution
             9
     """,
  examples = """
       Examples:
         > SELECT _FUNC_(622485130170302463l, 9);
          617981530542964735
     """,
  since = "0.1.0")
case class ToParent(h3Expr: Expression, parentResolutionExpr: Expression)
  extends BinaryExpression with CodegenFallback with ImplicitCastInputTypes with NullIntolerant {

  override def left: Expression = h3Expr
  override def right: Expression = parentResolutionExpr
  override def inputTypes: Seq[DataType] = Seq(LongType, IntegerType)
  override def dataType: DataType = LongType

  override protected def nullSafeEval(h3Any: Any, parentResolutionAny: Any): Any = {
    val h3 = h3Any.asInstanceOf[Long]
    val parentResolution = parentResolutionAny.asInstanceOf[Int]
    H3.getInstance().h3ToParent(h3, parentResolution)
  }
}
