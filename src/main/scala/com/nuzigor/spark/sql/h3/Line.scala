/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.h3.H3
import com.uber.h3core.exceptions.LineUndefinedException
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, LongType}

import scala.collection.JavaConverters._

/**
 * Returns the line of indexes between start and end.
 *
 * @param startExpr h3 start.
 * @param endExpr h3 end.
 */
@ExpressionDescription(
  usage = "_FUNC_(start, end) - Returns the line of indexes between start and end.",
  arguments = """
       Arguments:
         * start - start h3 index
             622485130170302463l
         * end - end h3 index
             622485130170957823l
     """,
  examples = """
       Examples:
         > SELECT _FUNC_(622485130170302463l, 622485130170957823l);
          [622485130170302463,622485130170171391,622485130170204159,622485130171088895,622485130170957823]
     """,
  group = "array_funcs",
  since = "0.1.0")
case class Line(startExpr: Expression, endExpr: Expression,
                failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends BinaryExpression with CodegenFallback with ImplicitCastInputTypes with NullIntolerant {

  def this(startExpr: Expression, endExpr: Expression) =
    this(startExpr, endExpr, SQLConf.get.ansiEnabled)

  override def left: Expression = startExpr
  override def right: Expression = endExpr
  override def inputTypes: Seq[DataType] = Seq(LongType, LongType)
  override def dataType: DataType = ArrayType(LongType, containsNull = false)
  override def nullable: Boolean = !failOnError || super.nullable

  override protected def nullSafeEval(startAny: Any, endAny: Any): Any = {
    val start = startAny.asInstanceOf[Long]
    val end = endAny.asInstanceOf[Long]
    try {
      ArrayData.toArrayData(H3.getInstance().h3Line(start, end).asScala.toArray)
    }
    catch {
      case _: LineUndefinedException if !failOnError => null
    }
  }
}
