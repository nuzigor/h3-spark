/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

// scalastyle:off

package com.nuzigor.spark.sql

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.{SQLContext, SparkSession}

package object h3 {
  def registerAll(sqlContext: SQLContext): Unit = {
    registerAll(sqlContext.sparkSession)
  }

  def registerAll(sparkSession: SparkSession): Unit = {
    val functionRegistry = sparkSession.sessionState.functionRegistry
    expressions.foreach {
      case (name, builder) => functionRegistry.createOrReplaceTempFunction(name, builder)
    }
  }

  def dropAll(sparkSession: SparkSession): Unit = {
    val functionRegistry = sparkSession.sessionState.functionRegistry
    expressions.foreach {
      case (name, _) => functionRegistry.dropFunction(FunctionIdentifier(name))
    }
  }

  private type UnaryFunctionBuilder = Expression => Expression
  private type BinaryFunctionBuilder = (Expression, Expression) => Expression
  private type TernaryFunctionBuilder = (Expression, Expression, Expression) => Expression

  private val expressions: Map[String, FunctionBuilder] = Map(
    expression("from_geo", FromGeo),
    expression("from_wkt", FromWkt),
    expression("array_from_wkt", ArrayFromWkt),
    expression("k_ring", KRing),
    expression("hex_ring", HexRing),
    expression("line", Line),
    expression("distance", Distance),
    expression("get_resolution", GetResolution),
    expression("is_valid", IsValid),
    expression("to_parent", ToParent),
    expression("to_children", ToChildren),
    expression("to_center_child", ToCenterChild),
    expression("compact", Compact),
    expression("uncompact", Uncompact),
  )

  private def expression(name: String, unaryFunctionBuilder: UnaryFunctionBuilder) : (String, FunctionBuilder) = {
    val builder : FunctionBuilder = expressions => {
      assert(expressions.size == 1)
      unaryFunctionBuilder(expressions.head)
    }

    (s"h3_$name", builder)
  }

  private def expression(name: String, binaryFunctionBuilder: BinaryFunctionBuilder) : (String, FunctionBuilder) = {
    val builder: FunctionBuilder = expressions => {
      assert(expressions.size == 2)
      binaryFunctionBuilder(expressions.head, expressions(1))
    }

    (s"h3_$name", builder)
  }

  private def expression(name: String, ternaryFunctionBuilder: TernaryFunctionBuilder) : (String, FunctionBuilder) = {
    val builder : FunctionBuilder = expressions => {
      assert(expressions.size == 3)
      ternaryFunctionBuilder(expressions.head, expressions(1), expressions(2))
    }

    (s"h3_$name", builder)
  }
}
// scalastyle:on
