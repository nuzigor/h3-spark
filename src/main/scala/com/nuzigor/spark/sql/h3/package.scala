/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql

import org.apache.spark.sql.{SQLContext, SparkSession}

package object h3 {
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
