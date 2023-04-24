/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.h3.H3
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ImplicitCastInputTypes, NullIntolerant, TernaryExpression}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType}

/**
 * Returns h3 address from latitude and longitude for at specific resolution.
 *
 * @param first the latitude.
 * @param second the longitude.
 * @param third h3 resolution
 */
@ExpressionDescription(
  usage = "_FUNC_(latitude, longitude, resolution) - Returns h3 address from latitude and longitude at target resolution.",
  arguments = """
       Arguments:
         * latitude - latitude in degrees
             81.34534d
         * longitude - longitude in degrees
             -164.64459d
         * resolution - h3 index resolution
             9
     """,
  examples = """
       Examples:
         > SELECT _FUNC_(81.34534d, -164.64459d, 9);
          617057114733412351
     """,
  since = "0.9.0"
)
case class FromLatLng(first: Expression, second: Expression, third: Expression, failOnError: Boolean = SQLConf.get.ansiEnabled)
    extends TernaryExpression
    with CodegenFallback
    with ImplicitCastInputTypes
    with NullIntolerant {

  def this(first: Expression, second: Expression, third: Expression) =
    this(first, second, third, SQLConf.get.ansiEnabled)

  override def inputTypes: Seq[DataType] = Seq(DoubleType, DoubleType, IntegerType)
  override def dataType: DataType = LongType
  override def nullable: Boolean = !failOnError || super.nullable

  override protected def nullSafeEval(latitudeAny: Any, longitudeAny: Any, resolutionAny: Any): Any = {
    val latitude = latitudeAny.asInstanceOf[Double]
    val longitude = longitudeAny.asInstanceOf[Double]
    val resolution = resolutionAny.asInstanceOf[Int]
    try {
      H3.getInstance().latLngToCell(latitude, longitude, resolution)
    } catch {
      case _: IllegalArgumentException if !failOnError => null
    }
  }

  override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): FromLatLng =
    copy(first = newFirst, second = newSecond, third = newThird)
}
