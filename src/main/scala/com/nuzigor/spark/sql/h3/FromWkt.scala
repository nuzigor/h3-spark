/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.h3.H3
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ImplicitCastInputTypes, Literal, NullIntolerant}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.io.{ParseException, WKTReader}

/**
 * Return h3 address from a WKT string
 *
 * @param wktExpr point in WKT format.
 * @param resolutionExpr h3 resolution
 */
case class FromWkt(wktExpr: Expression, resolutionExpr: Expression)
  extends BinaryExpression with CodegenFallback with ImplicitCastInputTypes with NullIntolerant {

  override def left: Expression = wktExpr
  override def right: Expression = resolutionExpr
  override def inputTypes: Seq[DataType] = Seq(StringType, IntegerType)
  override def dataType: DataType = LongType
  override def nullable: Boolean = true

  override protected def nullSafeEval(wktAny: Any, resolutionAny: Any): Any = {
    val wkt = wktAny.asInstanceOf[UTF8String].toString
    val resolution = resolutionAny.asInstanceOf[Int]
    try {
      val reader = new WKTReader()
      val geometry = reader.read(wkt)
      if (geometry.isEmpty) {
        null
      } else {
        val coordinate = geometry.getCoordinate
        if (coordinate == null) {
          null
        } else {
          H3.getInstance().geoToH3(coordinate.y, coordinate.x, resolution)
        }
      }
    } catch {
      case _: ParseException => null
    }
  }
}
