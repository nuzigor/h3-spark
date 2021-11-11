/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.h3.H3
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, IntegerType, LongType}

/**
 * Returns the center child of h3 index at child resolution.
 *
 * @param h3Expr h3 index.
 * @param childResolutionExpr child resolution.
 */
@ExpressionDescription(
  usage = "_FUNC_(h3, resolution) - Returns the center child of h3 index at child resolution.",
  arguments = """
       Arguments:
         * h3 - parent h3 index
             622485130170302463l
         * resolution - child index resolution
             12
     """,
  examples = """
       Examples:
         > SELECT _FUNC_(622485130170302463l, 12);
          631492329425011199
     """,
  since = "0.1.0")
case class ToCenterChild(h3Expr: Expression, childResolutionExpr: Expression,
                         failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends BinaryExpression with CodegenFallback with ImplicitCastInputTypes with NullIntolerant {

  def this(h3Expr: Expression, childResolutionExpr: Expression) =
    this(h3Expr, childResolutionExpr, SQLConf.get.ansiEnabled)

  override def left: Expression = h3Expr
  override def right: Expression = childResolutionExpr
  override def inputTypes: Seq[DataType] = Seq(LongType, IntegerType)
  override def dataType: DataType = LongType
  override def nullable: Boolean = !failOnError || super.nullable

  override protected def nullSafeEval(h3Any: Any, childResolutionAny: Any): Any = {
    val h3 = h3Any.asInstanceOf[Long]
    val childResolution = childResolutionAny.asInstanceOf[Int]
    try {
      H3.getInstance().h3ToCenterChild(h3, childResolution)
    }
    catch {
      case _: IllegalArgumentException if !failOnError => null
    }
  }
}
