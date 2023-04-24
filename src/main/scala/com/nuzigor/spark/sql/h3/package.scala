/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql

import org.apache.spark.sql.{SQLContext, SparkSession}

// scalastyle:off
package object h3 {
// scalastyle:on
  def registerAll(sqlContext: SQLContext): Unit = {
    registerAll(sqlContext.sparkSession)
  }

  def registerAll(sparkSession: SparkSession): Unit = {
    val functionRegistry = sparkSession.sessionState.functionRegistry
    FunctionCatalog.functions.foreach { case (functionId, info, builder) =>
      functionRegistry.registerFunction(functionId, info, builder)
    }
  }

  def dropAll(sparkSession: SparkSession): Unit = {
    val functionRegistry = sparkSession.sessionState.functionRegistry
    FunctionCatalog.functions.foreach { case (functionId, _, _) =>
      functionRegistry.dropFunction(functionId)
    }
  }
}
