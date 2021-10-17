/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, LongType}

import java.util
import scala.collection.JavaConverters._

/**
 * Uncompacts the set of indices using the target resolution.
 *
 * @param h3Expr h3 origin.
 * @param resolutionExpr h3 resolution
 */
case class Uncompact(h3Expr: Expression, resolutionExpr: Expression)
  extends BinaryExpression with CodegenFallback with ImplicitCastInputTypes with NullIntolerant {

  override def left: Expression = h3Expr
  override def right: Expression = resolutionExpr
  override def inputTypes: Seq[DataType] = Seq(ArrayType(LongType), IntegerType)
  override def dataType: DataType = ArrayType(LongType)
  override def nullable: Boolean = left.nullable || right.nullable || left.dataType.asInstanceOf[ArrayType].containsNull

  override protected def nullSafeEval(h3Any: Any, resolutionAny: Any): Any = {
    val list = new util.ArrayList[java.lang.Long]()
    val resolution = resolutionAny.asInstanceOf[Int]
    var nullFound = false
    h3Any.asInstanceOf[ArrayData].foreach(LongType, (_, v) =>
      if (v == null) {
        nullFound = true
      } else {
        list.add(v.asInstanceOf[Long])
      }
    )

    if (nullFound) {
      null
    } else {
      new GenericArrayData(H3.getInstance().uncompact(list, resolution).asScala)
    }
  }
}
