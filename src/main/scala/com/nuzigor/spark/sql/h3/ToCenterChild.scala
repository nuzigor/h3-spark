/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, LongType}

import scala.collection.JavaConverters._

/**
 * Returns the center child of h3 index at child resolution.
 *
 * @param h3Expr h3 index.
 * @param childResolutionExpr child resolution.
 */
case class ToCenterChild(h3Expr: Expression, childResolutionExpr: Expression)
  extends BinaryExpression with CodegenFallback with ImplicitCastInputTypes with NullIntolerant {

  override def left: Expression = h3Expr
  override def right: Expression = childResolutionExpr
  override def inputTypes: Seq[DataType] = Seq(LongType, IntegerType)
  override def dataType: DataType = LongType

  override protected def nullSafeEval(h3Any: Any, childResolutionAny: Any): Any = {
    val h3 = h3Any.asInstanceOf[Long]
    val childResolution = childResolutionAny.asInstanceOf[Int]
    H3.getInstance().h3ToCenterChild(h3, childResolution)
  }
}
