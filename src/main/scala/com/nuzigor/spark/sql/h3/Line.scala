/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.uber.h3core.exceptions.LineUndefinedException
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, LongType}

import scala.collection.JavaConverters._

/**
 * Returns the line of indexes between start and end.
 *
 * @param startExpr h3 start.
 * @param endExpr h3 end.
 */
case class Line(startExpr: Expression, endExpr: Expression)
  extends BinaryExpression with CodegenFallback with ImplicitCastInputTypes with NullIntolerant {

  override def left: Expression = startExpr
  override def right: Expression = endExpr
  override def inputTypes: Seq[DataType] = Seq(LongType, LongType)
  override def dataType: DataType = ArrayType(LongType)
  override def nullable: Boolean = true

  override protected def nullSafeEval(startAny: Any, endAny: Any): Any = {
    val start = startAny.asInstanceOf[Long]
    val end = endAny.asInstanceOf[Long]
    try {
      new GenericArrayData(H3.getInstance().h3Line(start, end).asScala)
    }
    catch {
      case _: LineUndefinedException => null
    }
  }
}