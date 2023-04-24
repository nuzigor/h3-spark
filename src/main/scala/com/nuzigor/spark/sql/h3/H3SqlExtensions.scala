/*
 * Copyright 2021 Igor Nuzhnov
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import org.apache.spark.sql.SparkSessionExtensions

class H3SqlExtensions extends (SparkSessionExtensions => Unit) {
  def apply(sse: SparkSessionExtensions): Unit = {
    FunctionCatalog.functions.foreach(sse.injectFunction)
  }
}
