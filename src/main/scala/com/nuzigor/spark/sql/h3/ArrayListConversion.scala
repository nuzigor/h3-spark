/*
 * Copyright 2021 Igor Nuzhnov
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import org.apache.spark.sql.catalyst.util.ArrayData

import java.util
import scala.annotation.tailrec

trait ArrayListConversion {
  protected def toLongArrayList(arrayData: ArrayData, nullEntries: Boolean): Option[util.ArrayList[java.lang.Long]] = {
    val numElements = arrayData.numElements
    if (nullEntries && hasNull(arrayData, numElements)) {
      None
    } else {
      val list = new util.ArrayList[java.lang.Long]
      for (i <- 0 until numElements) {
        list.add(arrayData.getLong(i))
      }
      Some(list)
    }
  }

  private def hasNull(arrayData: ArrayData, numElements: Int): Boolean = hasNull(arrayData, numElements, 0)

  @tailrec
  private def hasNull(arrayData: ArrayData, numElements: Int, index: Int): Boolean = {
    index < numElements && (arrayData.isNullAt(index) || hasNull(arrayData, numElements, index + 1))
  }
}
