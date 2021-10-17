/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType}

/**
 * Returns the resolution of h3 index.
 *
 * @param h3Expr h3 index.
 */
case class GetResolution(h3Expr: Expression)
  extends UnaryExpression with CodegenFallback with ImplicitCastInputTypes with NullIntolerant {

  override def child: Expression = h3Expr
  override def inputTypes: Seq[DataType] = Seq(LongType)
  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(h3Any: Any): Any = {
    val h3 = h3Any.asInstanceOf[Long]
    H3.getInstance().h3GetResolution(h3)
  }
}
