/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.h3.H3
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, LongType}

import java.util
import scala.collection.JavaConverters._

/**
 * Uncompacts the set of indices using the target resolution.
 *
 * @param h3Expr h3 indices.
 * @param resolutionExpr h3 resolution
 */
@ExpressionDescription(
  usage = "_FUNC_(h3, resolution) - Un-compacts the set of indices using the target resolution.",
  arguments = """
       Arguments:
         * h3 - array of h3 indices
             array(622485130170302463l, 622485130170957823l)
     """,
  examples = """
       Examples:
         > SELECT _FUNC_(h3_compact(h3_k_ring(622485130170302463l, 3)), 10);
          [622485130170761215,622485130170793983,622485130171482111,622485130151526399,...]
         > SELECT _FUNC_(array(0), 10);
          []
     """,
  group = "array_funcs",
  since = "0.1.0")
case class Uncompact(h3Expr: Expression, resolutionExpr: Expression,
                     failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends BinaryExpression with CodegenFallback with ImplicitCastInputTypes with NullIntolerant {

  def this(h3Expr: Expression, resolutionExpr: Expression) =
    this(h3Expr, resolutionExpr, SQLConf.get.ansiEnabled)

  override def left: Expression = h3Expr
  override def right: Expression = resolutionExpr
  override def inputTypes: Seq[DataType] = Seq(ArrayType(LongType), IntegerType)
  override def dataType: DataType = ArrayType(LongType, containsNull = false)
  override def nullable: Boolean =
    if (failOnError) left.nullable || right.nullable || left.dataType.asInstanceOf[ArrayType].containsNull else true

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
      try {
        ArrayData.toArrayData(H3.getInstance().uncompact(list, resolution).asScala.toArray)
      }
      catch {
        case _: IllegalArgumentException if !failOnError => null
      }
    }
  }
}
