/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.h3

import com.uber.h3core.H3Core

object H3 {
  @transient private lazy val instance: ThreadLocal[H3Core] = new ThreadLocal[H3Core] {
    override def initialValue(): H3Core = H3Core.newInstance()
  }

  def getInstance(): H3Core = instance.get()
}
