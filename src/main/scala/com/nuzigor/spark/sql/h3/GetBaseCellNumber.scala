/*
 * Copyright 2021 Igor Nuzhnov
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.h3.H3
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ImplicitCastInputTypes, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType}

/**
 * @param child h3 origin.
 */
@ExpressionDescription(
  usage = "_FUNC_(h3) - Returns the base cell number for h3 index.",
  arguments = """
       Arguments:
         * h3 -  h3 index
             622485130170302463l
     """,
  examples = """
       Examples:
         > SELECT _FUNC_(622485130170302463l);
         //TODO: Add output examples
     """,
  since = "0.9.2"
)
case class GetBaseCellNumber(child: Expression) extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {
  override def inputTypes: Seq[DataType] = Seq(LongType)
  override def dataType: DataType = IntegerType
  override def nullable: Boolean = true

  override protected def nullSafeEval(originAny: Any): Any = {
    val origin = originAny.asInstanceOf[Long]
    val baseCell = H3.getInstance().getBaseCellNumber(origin)
    if (0 <= baseCell && baseCell <= 121) {
      baseCell
    } else {
      null
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(
      ctx,
      ev,
      origin => {
        s"""
         |int h3BcOffset = 45;
         |long h3BcMask = 127L << h3BcOffset;
         |long initial = ($origin & h3BcMask) >> h3BcOffset;
         |${ev.value} = (${CodeGenerator.javaType(dataType)}) initial;
         |${ev.isNull} = !(0 <= ${ev.value} && ${ev.value} <= 121);
         |""".stripMargin
      }
    )
  }

  override protected def withNewChildInternal(newChild: Expression): GetBaseCellNumber = copy(child = newChild)
}
