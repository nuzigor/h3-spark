/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.h3.H3
import com.uber.h3core.AreaUnit
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ImplicitCastInputTypes, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType}

/**
 * Exact area of specific cell in area units.
 */
abstract class CellArea extends UnaryExpression with CodegenFallback with ImplicitCastInputTypes with NullIntolerant {

  def unit: AreaUnit

  override def inputTypes: Seq[DataType] = Seq(LongType)
  override def dataType: DataType = DoubleType

  override protected def nullSafeEval(h3Any: Any): Any = {
    val h3 = h3Any.asInstanceOf[Long]
    H3.getInstance().cellArea(h3, unit)
  }
}
