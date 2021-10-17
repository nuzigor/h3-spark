/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, LongType}

import scala.collection.JavaConverters._

/**
 * Returns h3 indices within k distance of the origin index.
 *
 * @param originExpr h3 origin.
 * @param kExpr k distance.
 */
case class KRing(originExpr: Expression, kExpr: Expression)
  extends BinaryExpression with CodegenFallback with ImplicitCastInputTypes with NullIntolerant {

  override def left: Expression = originExpr
  override def right: Expression = kExpr
  override def inputTypes: Seq[DataType] = Seq(LongType, IntegerType)
  override def dataType: DataType = ArrayType(LongType)

  override protected def nullSafeEval(originAny: Any, kAny: Any): Any = {
    val origin = originAny.asInstanceOf[Long]
    val k = kAny.asInstanceOf[Int]
    new GenericArrayData(H3.getInstance().kRing(origin, k).asScala)
  }
}
