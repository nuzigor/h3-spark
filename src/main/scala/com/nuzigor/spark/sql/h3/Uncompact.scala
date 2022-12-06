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

import scala.collection.JavaConverters._

/**
 * Uncompacts the set of indices using the target resolution.
 *
 * @param left h3 indices.
 * @param right h3 resolution
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
         > SELECT _FUNC_(h3_compact(h3_grid_disk(622485130170302463l, 3)), 10);
          [622485130170761215,622485130170793983,622485130171482111,622485130151526399,...]
         > SELECT _FUNC_(array(0), 10);
          []
     """,
  group = "array_funcs",
  since = "0.1.0")
case class Uncompact(left: Expression, right: Expression,
                     failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends BinaryExpression with CodegenFallback with ImplicitCastInputTypes with NullIntolerant with ArrayListConversion {

  def this(left: Expression, right: Expression) =
    this(left, right, SQLConf.get.ansiEnabled)

  @transient private lazy val nullEntries: Boolean = left.dataType.asInstanceOf[ArrayType].containsNull

  override def inputTypes: Seq[DataType] = Seq(ArrayType(LongType), IntegerType)
  override def dataType: DataType = ArrayType(LongType, containsNull = false)
  override def nullable: Boolean = !failOnError || left.nullable || right.nullable || nullEntries

  override protected def nullSafeEval(h3Any: Any, resolutionAny: Any): Any = {
    val h3Array = h3Any.asInstanceOf[ArrayData]
    val resolution = resolutionAny.asInstanceOf[Int]
    toLongArrayList(h3Array, nullEntries) match {
      case Some(list) =>
        try {
          ArrayData.toArrayData(H3.getInstance().uncompactCells(list, resolution).asScala.toArray)
        } catch {
          case _: IllegalArgumentException if !failOnError => null
        }

      case None => null
    }
  }

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Uncompact = copy(left = newLeft, right = newRight)
}
