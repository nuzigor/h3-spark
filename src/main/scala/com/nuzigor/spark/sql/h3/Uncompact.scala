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
import scala.annotation.tailrec
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

  @transient private lazy val nullEntries: Boolean = left.dataType.asInstanceOf[ArrayType].containsNull

  override def left: Expression = h3Expr
  override def right: Expression = resolutionExpr
  override def inputTypes: Seq[DataType] = Seq(ArrayType(LongType), IntegerType)
  override def dataType: DataType = ArrayType(LongType, containsNull = false)
  override def nullable: Boolean = !failOnError || left.nullable || right.nullable || nullEntries

  override protected def nullSafeEval(h3Any: Any, resolutionAny: Any): Any = {
    val h3Array = h3Any.asInstanceOf[ArrayData]
    val resolution = resolutionAny.asInstanceOf[Int]

    if (nullEntries && hasNull(h3Array)) {
      null
    } else {
      val list = new util.ArrayList[java.lang.Long]
      for (i <- 0 until h3Array.numElements) {
        list.add(h3Array.getLong(i))
      }

      try {
        ArrayData.toArrayData(H3.getInstance().uncompact(list, resolution).asScala.toArray)
      } catch {
        case _: IllegalArgumentException if !failOnError => null
      }
    }
  }

  private def hasNull(arrayData: ArrayData): Boolean = hasNull(arrayData, arrayData.numElements, 0)

  @tailrec
  private def hasNull(arrayData: ArrayData, numEntries: Int, index: Int): Boolean = {
    index < numEntries && (arrayData.isNullAt(index) || hasNull(arrayData, numEntries, index + 1))
  }
}
