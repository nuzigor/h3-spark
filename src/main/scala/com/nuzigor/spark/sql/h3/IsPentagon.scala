/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.h3.H3
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ImplicitCastInputTypes, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.types.{BooleanType, DataType, LongType}

/**
 * Returns true if it's a pentagon h3 index.
 *
 * @param h3Expr h3 index.
 */
@ExpressionDescription(
  usage = "_FUNC_(h3) - Returns true if it's a pentagon h3 index.",
  arguments = """
       Arguments:
         * h3 - h3 index
             622485130170302463l
     """,
  examples = """
       Examples:
         > SELECT _FUNC_(622485130170302463l);
          false
     """,
  since = "0.7.0")
case class IsPentagon(h3Expr: Expression)
  extends UnaryExpression with CodegenFallback with ImplicitCastInputTypes with NullIntolerant {

  override def child: Expression = h3Expr
  override def inputTypes: Seq[DataType] = Seq(LongType)
  override def dataType: DataType = BooleanType

  override protected def nullSafeEval(h3Any: Any): Any = {
    val h3 = h3Any.asInstanceOf[Long]
    H3.getInstance().h3IsPentagon(h3)
  }
}
