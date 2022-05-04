/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.h3.H3
import com.uber.h3core.exceptions.PentagonEncounteredException
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, LongType}

import scala.collection.JavaConverters._

/**
 * Returns the hollow hexagonal ring centered at origin with sides of length k.
 *
 * @param left h3 origin.
 * @param right k distance.
 */
@ExpressionDescription(
  usage = "_FUNC_(h3, k) - Returns the hollow hexagonal ring centered at origin with sides of length k.",
  arguments = """
       Arguments:
         * h3 - h3 index
             622485130170302463l
         * k - distance
             3
     """,
  examples = """
       Examples:
         > SELECT _FUNC_(622485130170302463l, 1);
          [622485130171252735,622485130171842559,622485130171711487,622485130170171391,622485130170105855,622485130170236927]
     """,
  group = "array_funcs",
  since = "0.1.0")
case class HexRing(left: Expression, right: Expression,
                   failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends BinaryExpression with CodegenFallback with ImplicitCastInputTypes with NullIntolerant {

  def this(left: Expression, right: Expression) =
    this(left, right, SQLConf.get.ansiEnabled)

  override def inputTypes: Seq[DataType] = Seq(LongType, IntegerType)
  override def dataType: DataType = ArrayType(LongType, containsNull = false)
  override def nullable: Boolean = !failOnError || super.nullable

  override protected def nullSafeEval(originAny: Any, kAny: Any): Any = {
    val origin = originAny.asInstanceOf[Long]
    val k = kAny.asInstanceOf[Int]
    try {
      ArrayData.toArrayData(H3.getInstance().hexRing(origin, k).asScala.toArray)
    } catch {
      case _: PentagonEncounteredException if !failOnError => null
    }
  }

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): HexRing = copy(left = newLeft, right = newRight)
}
