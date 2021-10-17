/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.{ArrayType, DataType, LongType}

import java.util
import scala.collection.JavaConverters._

/**
 * Compacts the set of indices as best as possible.
 *
 * @param h3Expr h3 origin.
 */
case class Compact(h3Expr: Expression)
  extends UnaryExpression with CodegenFallback with ImplicitCastInputTypes with NullIntolerant {

  override def child: Expression = h3Expr
  override def inputTypes: Seq[DataType] = Seq(ArrayType(LongType))
  override def dataType: DataType = ArrayType(LongType)
  override def nullable: Boolean = child.nullable || child.dataType.asInstanceOf[ArrayType].containsNull

  override protected def nullSafeEval(h3Any: Any): Any = {
    val list = new util.ArrayList[java.lang.Long]()
    h3Any.asInstanceOf[ArrayData].foreach(LongType, (_, v) =>
      if (v == null) {
        return null
      } else {
        list.add(v.asInstanceOf[Long])
      }
    )
    new GenericArrayData(H3.getInstance().compact(list).asScala)
  }
}
