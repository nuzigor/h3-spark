/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, NullIntolerant, TernaryExpression}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType}

/**
 * Return h3 address from a WKT string
 *
 * @param latitudeExpr the latitude.
 * @param longitudeExpr the longitude.
 * @param resolutionExpr h3 resolution
 */
case class FromGeo(latitudeExpr: Expression, longitudeExpr: Expression, resolutionExpr: Expression)
  extends TernaryExpression with CodegenFallback with ImplicitCastInputTypes with NullIntolerant {

  override def inputTypes: Seq[DataType] = Seq(DoubleType, DoubleType, IntegerType)
  override def dataType: DataType = LongType
  override def children: Seq[Expression] = Seq(latitudeExpr, longitudeExpr, resolutionExpr)

  override protected def nullSafeEval(latitudeAny: Any, longitudeAny: Any, resolutionAny: Any): Any = {
    val latitude = latitudeAny.asInstanceOf[Double]
    val longitude = longitudeAny.asInstanceOf[Double]
    val resolution = resolutionAny.asInstanceOf[Int]
    H3.getInstance().geoToH3(latitude, longitude, resolution)
  }
}
