/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.uber.h3core.exceptions.DistanceUndefinedException
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType}

/**
 * Returns the distance in grid cells between start and end.
 *
 * @param startExpr h3 start.
 * @param endExpr h3 end.
 */
case class Distance(startExpr: Expression, endExpr: Expression)
  extends BinaryExpression with CodegenFallback with ImplicitCastInputTypes with NullIntolerant {

  override def left: Expression = startExpr
  override def right: Expression = endExpr
  override def inputTypes: Seq[DataType] = Seq(LongType, LongType)
  override def dataType: DataType = IntegerType
  override def nullable: Boolean = true

  override protected def nullSafeEval(originAny: Any, endAny: Any): Any = {
    val start = originAny.asInstanceOf[Long]
    val end = endAny.asInstanceOf[Long]
    try {
      H3.getInstance().h3Distance(start, end)
    }
    catch {
      case _: DistanceUndefinedException => null
    }
  }
}
