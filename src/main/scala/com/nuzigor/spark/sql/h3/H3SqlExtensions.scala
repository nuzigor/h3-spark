package com.nuzigor.spark.sql.h3

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}

class H3SqlExtensions extends (SparkSessionExtensions => Unit) {
  def apply(sse: SparkSessionExtensions): Unit = {
    injectFunction(sse, "h3_from_geo", FromGeo)
    injectFunction(sse, "h3_from_wkt", FromWkt)
    injectFunction(sse, "h3_array_from_wkt", ArrayFromWkt)
    injectFunction(sse, "h3_k_ring", KRing)
    injectFunction(sse, "h3_hex_ring", HexRing)
    injectFunction(sse, "h3_line", Line)
    injectFunction(sse, "h3_distance", Distance)
    injectFunction(sse, "h3_get_resolution", GetResolution)
    injectFunction(sse, "h3_is_valid", IsValid)
    injectFunction(sse, "h3_to_parent", ToParent)
    injectFunction(sse, "h3_to_children", ToChildren)
    injectFunction(sse, "h3_to_center_child", ToCenterChild)
    injectFunction(sse, "h3_compact", Compact)
    injectFunction(sse, "h3_uncompact", Uncompact)
  }

  private type UnaryFunctionBuilder = Expression => Expression
  private type BinaryFunctionBuilder = (Expression, Expression) => Expression
  private type TernaryFunctionBuilder = (Expression, Expression, Expression) => Expression

  private def injectFunction(sse: SparkSessionExtensions, name: String, unaryFunctionBuilder: UnaryFunctionBuilder) : Unit = {
    val builder = (expressions: Seq[Expression]) => {
      assert(expressions.size == 1)
      unaryFunctionBuilder(expressions.head)
    }

    sse.injectFunction((
      FunctionIdentifier(name),
      new ExpressionInfo(unaryFunctionBuilder.getClass.getCanonicalName, name),
      builder))
  }

  private def injectFunction(sse: SparkSessionExtensions, name: String, binaryFunctionBuilder: BinaryFunctionBuilder) : Unit = {
    val builder = (expressions: Seq[Expression]) => {
      assert(expressions.size == 2)
      binaryFunctionBuilder(expressions.head, expressions.last)
    }

    sse.injectFunction((
      FunctionIdentifier(name),
      new ExpressionInfo(binaryFunctionBuilder.getClass.getCanonicalName, name),
      builder))
  }

  private def injectFunction(sse: SparkSessionExtensions, name: String, ternaryFunctionBuilder: TernaryFunctionBuilder) : Unit = {
    val builder = (expressions: Seq[Expression]) => {
      assert(expressions.size == 3)
      ternaryFunctionBuilder(expressions.head, expressions(1), expressions.last)
    }

    sse.injectFunction((
      FunctionIdentifier(name),
      new ExpressionInfo(ternaryFunctionBuilder.getClass.getCanonicalName, name),
      builder))
  }
}