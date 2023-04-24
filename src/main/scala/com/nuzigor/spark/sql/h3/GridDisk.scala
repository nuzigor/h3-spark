/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.h3.H3
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, LongType}

import scala.collection.JavaConverters._

/**
 * Returns h3 indices within k distance of the origin index.
 *
 * @param left h3 origin.
 * @param right k distance.
 */
@ExpressionDescription(
  usage = "_FUNC_(h3, k) - Returns h3 indices within k distance of the origin index.",
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
          [622485130170302463,622485130171842559,622485130171711487,622485130170171391,622485130170105855,622485130170236927,622485130171252735]
     """,
  group = "array_funcs",
  since = "0.9.0"
)
case class GridDisk(left: Expression, right: Expression) extends BinaryExpression with CodegenFallback with ImplicitCastInputTypes with NullIntolerant {

  override def inputTypes: Seq[DataType] = Seq(LongType, IntegerType)
  override def dataType: DataType = ArrayType(LongType, containsNull = false)

  override protected def nullSafeEval(originAny: Any, kAny: Any): Any = {
    val origin = originAny.asInstanceOf[Long]
    val k = kAny.asInstanceOf[Int]
    ArrayData.toArrayData(H3.getInstance().gridDisk(origin, k).asScala.toArray)
  }

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): GridDisk =
    copy(left = newLeft, right = newRight)
}
