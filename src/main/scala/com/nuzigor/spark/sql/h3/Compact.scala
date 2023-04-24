/*
 * Copyright 2021 Igor Nuzhnov
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.h3.H3
import com.uber.h3core.exceptions.H3Exception
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ImplicitCastInputTypes, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, LongType}

import scala.collection.JavaConverters._

/**
 * Compacts the set of indices as best as possible.
 *
 * @param child h3 indices.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the compacted `expr` array on indices.",
  arguments = """
       Arguments:
         * expr - array of h3 indices
             array(622485130170302463l, 622485130170957823l)
     """,
  examples = """
       Examples:
         > SELECT _FUNC_(h3_grid_disk(622485130170302463l, 3));
          [622485130170761215,622485130170793983,622485130171482111,622485130151526399,...]
         > SELECT _FUNC_(array(0));
          []
     """,
  group = "array_funcs",
  since = "0.1.0"
)
case class Compact(child: Expression, failOnError: Boolean = SQLConf.get.ansiEnabled)
    extends UnaryExpression
    with CodegenFallback
    with ImplicitCastInputTypes
    with NullIntolerant
    with ArrayListConversion {

  def this(child: Expression) = this(child, SQLConf.get.ansiEnabled)

  @transient private lazy val nullEntries: Boolean = child.dataType.asInstanceOf[ArrayType].containsNull

  override def inputTypes: Seq[DataType] = Seq(ArrayType(LongType))
  override def dataType: DataType = ArrayType(LongType, containsNull = false)
  override def nullable: Boolean = !failOnError || child.nullable || nullEntries

  override protected def nullSafeEval(h3Any: Any): Any = {
    val h3Array = h3Any.asInstanceOf[ArrayData]
    toLongArrayList(h3Array, nullEntries) match {
      case Some(list) =>
        try {
          ArrayData.toArrayData(H3.getInstance().compactCells(list).asScala.toArray)
        } catch {
          case _: H3Exception if !failOnError => null
        }

      case None => null
    }
  }

  override protected def withNewChildInternal(newChild: Expression): Compact = copy(child = newChild)
}
