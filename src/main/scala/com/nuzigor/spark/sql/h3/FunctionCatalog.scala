/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ExpressionInfo}

import scala.reflect.ClassTag

object FunctionCatalog {
  lazy val functions: Seq[(FunctionIdentifier, ExpressionInfo, FunctionBuilder)] = Seq(
    createFunctionDescription[FromGeo]("h3_from_geo"),
    createFunctionDescription[FromWkt]("h3_from_wkt"),
    createFunctionDescription[ArrayFromWkt]("h3_array_from_wkt"),
    createFunctionDescription[KRing]("h3_k_ring"),
    createFunctionDescription[HexRing]("h3_hex_ring"),
    createFunctionDescription[Line]("h3_line"),
    createFunctionDescription[Distance]("h3_distance"),
    createFunctionDescription[GetResolution]("h3_get_resolution"),
    createFunctionDescription[IsValid]("h3_is_valid"),
    createFunctionDescription[ToParent]("h3_to_parent"),
    createFunctionDescription[ToChildren]("h3_to_children"),
    createFunctionDescription[ToCenterChild]("h3_to_center_child"),
    createFunctionDescription[Compact]("h3_compact"),
    createFunctionDescription[Uncompact]("h3_uncompact"),
  )

  private def createFunctionDescription[T <: Expression](name: String)
                                                        (implicit tag: ClassTag[T]): (FunctionIdentifier, ExpressionInfo, FunctionBuilder) = {
    val constructors = tag.runtimeClass.getConstructors
    val builder = (expressions: Seq[Expression]) => {
      val params = Seq.fill(expressions.size)(classOf[Expression])
      val f = constructors.find(_.getParameterTypes.toSeq == params).getOrElse {
        val validParametersCount = constructors
          .filter(_.getParameterTypes.forall(_ == classOf[Expression]))
          .map(_.getParameterCount).distinct.sorted
        val invalidArgumentsMsg = if (validParametersCount.length == 0) {
          s"Invalid arguments for function $name"
        } else {
          val expectedNumberOfParameters = if (validParametersCount.length == 1) {
            validParametersCount.head.toString
          } else {
            validParametersCount.init.mkString("one of ", ", ", " and ") +
              validParametersCount.last
          }
          s"Invalid number of arguments for function $name. " +
            s"Expected: $expectedNumberOfParameters; Found: ${params.length}"
        }
        throw new Exception(invalidArgumentsMsg)
      }

      try {
        f.newInstance(expressions: _*).asInstanceOf[Expression]
      } catch {
        case e: Exception => throw new Exception(e.getCause.getMessage)
      }
    }

    val expressionInfo = createExpressionInfo[T](name)
    (FunctionIdentifier(name), expressionInfo, builder)
  }

  def createExpressionInfo[T <: Expression : ClassTag](name: String): ExpressionInfo = {
    val clazz = scala.reflect.classTag[T].runtimeClass
    val ed = clazz.getAnnotation(classOf[ExpressionDescription])
    if (ed != null) {
      new ExpressionInfo(
        clazz.getCanonicalName,
        null,
        name,
        ed.usage(),
        ed.arguments(),
        ed.examples(),
        ed.note(),
        ed.group(),
        ed.since(),
        ed.deprecated())
    } else {
      new ExpressionInfo(clazz.getCanonicalName, name)
    }
  }
}
