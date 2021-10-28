/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.h3.H3
import com.uber.h3core.LengthUnit
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType}

/**
 * Gives the "great circle" or "haversine" distance between centers of h3 indices in units.
 */
trait PointDistance
  extends BinaryExpression with CodegenFallback with ImplicitCastInputTypes with NullIntolerant {

  def startExpr: Expression
  def endExpr: Expression
  def unit: LengthUnit

  override def left: Expression = startExpr
  override def right: Expression = endExpr
  override def inputTypes: Seq[DataType] = Seq(LongType, LongType)
  override def dataType: DataType = DoubleType

  override protected def nullSafeEval(originAny: Any, endAny: Any): Any = {
    val start = originAny.asInstanceOf[Long]
    val end = endAny.asInstanceOf[Long]
    val h3Instance = H3.getInstance()
    val startGeo = h3Instance.h3ToGeo(start)
    val endGeo = h3Instance.h3ToGeo(end)
    h3Instance.pointDist(startGeo, endGeo, unit)
  }
}
