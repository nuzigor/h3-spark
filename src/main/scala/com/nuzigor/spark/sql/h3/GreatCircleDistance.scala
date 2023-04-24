/*
 * Copyright 2021 Igor Nuzhnov
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.h3.H3
import com.uber.h3core.LengthUnit
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType}

/**
 * Gives the "great circle" or "haversine" distance between centers of h3 indices in units.
 */
abstract class GreatCircleDistance extends BinaryExpression with CodegenFallback with ImplicitCastInputTypes with NullIntolerant {

  def unit: LengthUnit

  override def inputTypes: Seq[DataType] = Seq(LongType, LongType)
  override def dataType: DataType = DoubleType

  override protected def nullSafeEval(originAny: Any, endAny: Any): Any = {
    val start = originAny.asInstanceOf[Long]
    val end = endAny.asInstanceOf[Long]
    val h3Instance = H3.getInstance()
    val startGeo = h3Instance.cellToLatLng(start)
    val endGeo = h3Instance.cellToLatLng(end)
    h3Instance.greatCircleDistance(startGeo, endGeo, unit)
  }
}
